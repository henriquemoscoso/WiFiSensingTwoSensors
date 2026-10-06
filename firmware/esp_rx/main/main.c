/*
 * esp_rx - ESP32 receptora (STA) para sensoriamento via CSI
 *
 * Funcao: conecta no AP da esp_tx, registra o callback de CSI e, para cada
 * frame recebido do TX, imprime UMA linha na serial:
 *
 *   CSI_US,<seq>,<t_us>,<rssi>,<len>,<v0>,<v1>,...,<v(len-1)>
 *
 *   seq   : contador local de CSI aceitos (saltos = amostras descartadas por fila cheia)
 *   t_us  : esp_timer_get_time() no momento do callback (us, 64 bits, sem wrap)
 *   rssi  : dBm
 *   len   : numero de bytes de CSI (128 = 64 subportadoras do L-LTF)
 *   v_i   : int8 brutos, em pares [imag, real] por subportadora (ordem da Espressif)
 *
 * Linhas que comecam com '#' ou com logs do IDF ('I (', 'W (') sao diagnostico,
 * e o parser no PC deve ignorar tudo que nao comeca com "CSI_US".
 *
 * Em paralelo, um socket UDP drena os pacotes do TX (porta 5005) e usa o 'seq'
 * do proprio TX para medir a perda real no enlace.
 */

#include <string.h>
#include <stdio.h>
#include <inttypes.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_log.h"
#include "esp_timer.h"
#include "esp_mac.h"
#include "esp_netif.h"
#include "nvs_flash.h"
#include "lwip/sockets.h"

/* ======================= CONFIGURACAO ======================= */
#define AP_SSID             "CSI_TX"           /* igual ao esp_tx */
#define AP_SENHA            "csi_sensing_01"   /* igual ao esp_tx */
#define AP_CANAL            6                  /* igual ao esp_tx (acelera a conexao) */
#define PORTA_UDP           5005               /* igual a PORTA_UDP_DESTINO do esp_tx */

#define FILTRAR_SO_HT       1    /* 1 = so frames HT 20 MHz (os UDP do TX); descarta beacons 11b */
#define CSI_MAX_BYTES       128  /* L-LTF: 64 subportadoras x (imag, real) */
#define TAMANHO_FILA        128  /* amostras em espera para a serial */
#define INTERVALO_STATS_S   5
/* ============================================================ */

#define PAYLOAD_MAGIC 0x43534954u              /* 'CSIT', mesmo valor do esp_tx */

static const char *TAG = "esp_rx";

typedef struct {
    int64_t  t_us;
    uint32_t seq;
    int8_t   rssi;
    uint16_t len;
    int8_t   dados[CSI_MAX_BYTES];
} amostra_csi_t;

typedef struct __attribute__((packed)) {      /* inicio do pacote_tx_t do esp_tx */
    uint32_t magic;
    uint32_t seq;
    int64_t  t_us;
} cabecalho_tx_t;

static QueueHandle_t s_fila = NULL;
static uint8_t s_bssid[6] = {0};
static volatile bool s_conectado = false;
static volatile bool s_aguardando_primeiro_csi = false;

/* contadores de diagnostico */
static volatile uint32_t s_csi_aceitos = 0;
static volatile uint32_t s_csi_fila_cheia = 0;
static volatile uint32_t s_csi_filtrados = 0;
static volatile uint32_t s_udp_recebidos = 0;
static volatile uint32_t s_udp_perdidos = 0;

/* ------------------------ Callback de CSI ------------------------ */
/* Roda no contexto da tarefa do WiFi: tem que ser rapido, so copia e enfileira. */
static void callback_csi(void *ctx, wifi_csi_info_t *info)
{
    static uint32_t seq = 0;

    if (!s_conectado || info == NULL || info->buf == NULL) {
        return;
    }
    if (memcmp(info->mac, s_bssid, 6) != 0) {
        return;                                    /* frame de outro transmissor */
    }
#if FILTRAR_SO_HT
    if (info->rx_ctrl.sig_mode != 1 || info->rx_ctrl.cwb != 0) {
        s_csi_filtrados++;                         /* beacon/11b/40 MHz */
        return;
    }
#endif

    amostra_csi_t a;
    a.t_us = esp_timer_get_time();
    a.seq  = seq++;
    a.rssi = info->rx_ctrl.rssi;
    a.len  = (info->len > CSI_MAX_BYTES) ? CSI_MAX_BYTES : info->len;
    memcpy(a.dados, info->buf, a.len);
    if (info->first_word_invalid && a.len >= 4) {
        memset(a.dados, 0, 4);                     /* 4 primeiros bytes invalidos pelo HW */
    }

    if (xQueueSend(s_fila, &a, 0) == pdTRUE) {
        s_csi_aceitos++;
    } else {
        s_csi_fila_cheia++;                        /* aparece como salto de seq no PC */
    }
}

/* -------------------- Tarefa de saida serial --------------------- */
static void tarefa_serial(void *arg)
{
    static char linha[64 + CSI_MAX_BYTES * 5 + 2];
    amostra_csi_t a;

    while (1) {
        if (xQueueReceive(s_fila, &a, portMAX_DELAY) != pdTRUE) {
            continue;
        }
        if (s_aguardando_primeiro_csi) {
            s_aguardando_primeiro_csi = false;
            printf("# ------------------------------------------------\n"
                   "# CSI OK: primeira amostra recebida (len=%u, rssi=%d dBm)\n"
                   "# ------------------------------------------------\n",
                   a.len, a.rssi);
        }
        int p = snprintf(linha, sizeof(linha), "CSI_US,%" PRIu32 ",%" PRId64 ",%d,%u",
                         a.seq, a.t_us, a.rssi, a.len);
        for (int i = 0; i < a.len && p < (int)sizeof(linha) - 6; i++) {
            p += snprintf(linha + p, sizeof(linha) - p, ",%d", a.dados[i]);
        }
        linha[p++] = '\n';
        fwrite(linha, 1, p, stdout);               /* uma escrita por linha */
        fflush(stdout);
    }
}

/* ------------- Tarefa UDP: drena pacotes e mede perda ------------- */
/* Sem um socket escutando, o lwIP responderia "ICMP port unreachable" a cada
 * pacote, gerando trafego extra no canal. */
static void tarefa_udp(void *arg)
{
    int sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_IP);
    if (sock < 0) {
        ESP_LOGE(TAG, "Falha ao criar socket UDP");
        vTaskDelete(NULL);
        return;
    }
    struct sockaddr_in end = {
        .sin_family = AF_INET,
        .sin_port = htons(PORTA_UDP),
        .sin_addr.s_addr = htonl(INADDR_ANY),
    };
    if (bind(sock, (struct sockaddr *)&end, sizeof(end)) < 0) {
        ESP_LOGE(TAG, "Falha no bind da porta %d", PORTA_UDP);
        close(sock);
        vTaskDelete(NULL);
        return;
    }

    uint8_t buf[128];
    bool primeiro = true;
    uint32_t ultimo_seq = 0;

    while (1) {
        int n = recv(sock, buf, sizeof(buf), 0);
        if (n < (int)sizeof(cabecalho_tx_t)) {
            continue;
        }
        cabecalho_tx_t h;
        memcpy(&h, buf, sizeof(h));
        if (h.magic != PAYLOAD_MAGIC) {
            continue;
        }
        s_udp_recebidos++;
        if (!primeiro) {
            if (h.seq > ultimo_seq + 1) {
                s_udp_perdidos += h.seq - ultimo_seq - 1;
            }
            /* h.seq <= ultimo_seq: TX reiniciou, so ressincroniza */
        }
        ultimo_seq = h.seq;
        primeiro = false;
    }
}

/* ---------------------- Tarefa de estatisticas -------------------- */
static void tarefa_stats(void *arg)
{
    uint32_t csi0 = 0, cheia0 = 0, filt0 = 0, udp0 = 0, perd0 = 0;

    while (1) {
        vTaskDelay(pdMS_TO_TICKS(INTERVALO_STATS_S * 1000));

        uint32_t csi = s_csi_aceitos, cheia = s_csi_fila_cheia, filt = s_csi_filtrados;
        uint32_t udp = s_udp_recebidos, perd = s_udp_perdidos;

        uint32_t d_udp = udp - udp0, d_perd = perd - perd0;
        float perda_pct = (d_udp + d_perd) ? 100.0f * d_perd / (d_udp + d_perd) : 0.0f;

        printf("# STATS csi_hz=%.1f udp_hz=%.1f perda_udp=%.1f%% fila_cheia=%" PRIu32
               " filtrados=%" PRIu32 " conectado=%d\n",
               (csi - csi0) / (float)INTERVALO_STATS_S,
               d_udp / (float)INTERVALO_STATS_S,
               perda_pct, cheia - cheia0, filt - filt0, s_conectado);

        csi0 = csi; cheia0 = cheia; filt0 = filt; udp0 = udp; perd0 = perd;
    }
}

/* ----------------------------- Eventos ----------------------------- */
static void tratar_eventos(void *arg, esp_event_base_t base, int32_t id, void *dados)
{
    if (base == WIFI_EVENT && id == WIFI_EVENT_STA_START) {
        /* Nao conecta aqui: a conexao e feita no fim de iniciar_wifi_sta(),
         * depois de banda, power-save e CSI estarem configurados. */
        ESP_LOGI(TAG, "STA iniciada");

    } else if (base == WIFI_EVENT && id == WIFI_EVENT_STA_CONNECTED) {
        wifi_event_sta_connected_t *e = (wifi_event_sta_connected_t *)dados;
        memcpy(s_bssid, e->bssid, 6);
        s_conectado = true;
        printf("# [1/3] Associado ao AP '%s' (" MACSTR ") no canal %d\n",
               AP_SSID, MAC2STR(s_bssid), e->channel);

    } else if (base == WIFI_EVENT && id == WIFI_EVENT_STA_DISCONNECTED) {
        wifi_event_sta_disconnected_t *e = (wifi_event_sta_disconnected_t *)dados;
        s_conectado = false;
        s_aguardando_primeiro_csi = false;
        printf("# !!! DESCONECTADO do TX (motivo=%d), tentando reconectar...\n", e->reason);
        esp_wifi_connect();

    } else if (base == IP_EVENT && id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *e = (ip_event_got_ip_t *)dados;
        s_aguardando_primeiro_csi = true;
        printf("# ================================================\n"
               "# [2/3] CONEXAO BEM-SUCEDIDA com o TX\n"
               "#       IP da RX: " IPSTR "   Gateway (TX): " IPSTR "\n"
               "#       Aguardando primeira amostra de CSI... [3/3]\n"
               "# ================================================\n",
               IP2STR(&e->ip_info.ip), IP2STR(&e->ip_info.gw));
    }
}

/* ------------------------- WiFi (STA) + CSI ------------------------ */
static void iniciar_wifi_sta(void)
{
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_create_default_wifi_sta();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &tratar_eventos, NULL));
    ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &tratar_eventos, NULL));

    wifi_country_t pais = {
        .cc = "BR", .schan = 1, .nchan = 13, .policy = WIFI_COUNTRY_POLICY_MANUAL,
    };
    ESP_ERROR_CHECK(esp_wifi_set_country(&pais));

    ESP_ERROR_CHECK(esp_wifi_set_storage(WIFI_STORAGE_RAM));
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));

    wifi_config_t sta_cfg = {
        .sta = {
            .ssid = AP_SSID,
            .password = AP_SENHA,
            .channel = AP_CANAL,
            .threshold.authmode = WIFI_AUTH_WPA2_PSK,
            .pmf_cfg = { .capable = true, .required = false },
        },
    };
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &sta_cfg));
    ESP_ERROR_CHECK(esp_wifi_set_protocol(WIFI_IF_STA,
                    WIFI_PROTOCOL_11B | WIFI_PROTOCOL_11G | WIFI_PROTOCOL_11N));

    ESP_ERROR_CHECK(esp_wifi_start());
    ESP_ERROR_CHECK(esp_wifi_set_bandwidth(WIFI_IF_STA, WIFI_BW_HT20));
    ESP_ERROR_CHECK(esp_wifi_set_ps(WIFI_PS_NONE));

    /* CSI: so L-LTF (64 subportadoras), SEM filtro de canal do HW
     * (o filtro suaviza subportadoras vizinhas e distorce a estatistica) */
    wifi_csi_config_t csi_cfg = {
        .lltf_en           = true,
        .htltf_en          = false,
        .stbc_htltf2_en    = false,
        .ltf_merge_en      = false,
        .channel_filter_en = false,
        .manu_scale        = false,
        .shift             = 0,
    };
    ESP_ERROR_CHECK(esp_wifi_set_csi_config(&csi_cfg));
    ESP_ERROR_CHECK(esp_wifi_set_csi_rx_cb(&callback_csi, NULL));
    ESP_ERROR_CHECK(esp_wifi_set_csi(true));

    /* Tudo configurado: agora sim conecta no TX */
    ESP_ERROR_CHECK(esp_wifi_connect());
}

/* ------------------------------ main ------------------------------ */
void app_main(void)
{
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    esp_log_level_set("wifi", ESP_LOG_WARN);       /* menos ruido na serial */

    printf("# ================================================\n"
           "# esp_rx  v4  (compilado em %s %s)\n"
           "# ================================================\n",
           __DATE__, __TIME__);

    /* 1) Fila primeiro: o callback de CSI depende dela */
    s_fila = xQueueCreate(TAMANHO_FILA, sizeof(amostra_csi_t));
    configASSERT(s_fila);

    /* 2) Pilha de rede (esp_netif_init/lwIP) ANTES de qualquer socket */
    iniciar_wifi_sta();

    /* 3) So agora as tarefas, incluindo a que abre o socket UDP */
    xTaskCreate(tarefa_serial, "tarefa_serial", 4096, NULL, 6, NULL);
    xTaskCreate(tarefa_udp,    "tarefa_udp",    3072, NULL, 4, NULL);
    xTaskCreate(tarefa_stats,  "tarefa_stats",  3072, NULL, 2, NULL);

    ESP_LOGI(TAG, "Aguardando conexao com %s...", AP_SSID);
}