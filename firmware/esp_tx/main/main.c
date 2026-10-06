/*
 * esp_tx - ESP32 transmissora (Access Point) para sensoriamento via CSI
 *
 * Funcao: sobe um AP WiFi em canal fixo / HT20 e, assim que a ESP32 RX (STA)
 * conecta e recebe IP, envia pacotes UDP unicast a uma taxa fixa (TAXA_ENVIO_HZ).
 * Cada pacote gera um frame OFDM na RX, que extrai o CSI dele.
 *
 * Payload de cada pacote: magic | seq | t_us (timestamp do TX) | enchimento
 * -> a RX usa 'seq' para medir perda e 't_us' para jitter/taxa real.
 */

#include <string.h>
#include <inttypes.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "esp_system.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_log.h"
#include "esp_timer.h"
#include "esp_mac.h"
#include "esp_netif.h"
#include "nvs_flash.h"
#include "lwip/sockets.h"
#include "lwip/inet.h"

/* ======================= CONFIGURACAO ======================= */
#define AP_SSID             "CSI_TX"
#define AP_SENHA            "csi_sensing_01"   /* minimo 8 caracteres */
#define AP_CANAL            6                  /* canal fixo (evite o canal do WiFi da UnB) */
#define AP_MAX_CONEXOES     1                  /* so a RX */

#define TAXA_ENVIO_HZ       100                /* pacotes por segundo */
#define PORTA_UDP_DESTINO   5005
#define TAMANHO_ENCHIMENTO  32                 /* bytes extras no payload */

#define POTENCIA_TX_QDBM    78                 /* unidade 0.25 dBm -> 78 = 19.5 dBm */
#define USAR_TAXA_FIXA      1                  /* 1 = forca HT MCS0 (recomendado p/ CSI) */
#define INTERVALO_LOG_S     5                  /* estatisticas a cada N segundos */
/* ============================================================ */

#if USAR_TAXA_FIXA
#include "esp_private/wifi.h"                  /* esp_wifi_internal_set_fix_rate */
#endif

#define PAYLOAD_MAGIC 0x43534954u              /* 'CSIT' */

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t seq;
    int64_t  t_us;
    uint8_t  enchimento[TAMANHO_ENCHIMENTO];
} pacote_tx_t;

static const char *TAG = "esp_tx";

static volatile uint32_t s_ip_sta = 0;         /* IP da RX (0 = ninguem conectado) */
static TaskHandle_t s_tarefa_envio = NULL;

/* --------------------------- Eventos --------------------------- */
static void tratar_eventos(void *arg, esp_event_base_t base, int32_t id, void *dados)
{
    if (base == WIFI_EVENT && id == WIFI_EVENT_AP_STACONNECTED) {
        wifi_event_ap_staconnected_t *e = (wifi_event_ap_staconnected_t *)dados;
        ESP_LOGI(TAG, "RX conectada: " MACSTR " (AID=%d)", MAC2STR(e->mac), e->aid);

    } else if (base == WIFI_EVENT && id == WIFI_EVENT_AP_STADISCONNECTED) {
        wifi_event_ap_stadisconnected_t *e = (wifi_event_ap_stadisconnected_t *)dados;
        s_ip_sta = 0;
        printf("# !!! RX DESCONECTADA (" MACSTR ") - envio pausado\n", MAC2STR(e->mac));

    } else if (base == IP_EVENT && id == IP_EVENT_AP_STAIPASSIGNED) {
        ip_event_ap_staipassigned_t *e = (ip_event_ap_staipassigned_t *)dados;
        s_ip_sta = e->ip.addr;
        printf("# ================================================\n"
               "# CONEXAO BEM-SUCEDIDA com a RX\n"
               "#   IP da RX: " IPSTR "\n"
               "#   Enviando pacotes UDP a %d Hz (porta %d)\n"
               "# ================================================\n",
               IP2STR(&e->ip), TAXA_ENVIO_HZ, PORTA_UDP_DESTINO);
    }
}

/* ------------------------- WiFi (AP) --------------------------- */
static void iniciar_wifi_ap(void)
{
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_create_default_wifi_ap();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    /* Sem agregacao AMPDU: cada pacote UDP vira um frame (= um CSI na RX),
     * e o driver passa a aceitar a taxa PHY fixa (MCS0). */
    cfg.ampdu_tx_enable = 0;
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &tratar_eventos, NULL));
    ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_AP_STAIPASSIGNED, &tratar_eventos, NULL));

    /* Regulatorio Brasil, canais 1-13, sem ajuste automatico */
    wifi_country_t pais = {
        .cc = "BR", .schan = 1, .nchan = 13, .policy = WIFI_COUNTRY_POLICY_MANUAL,
    };
    ESP_ERROR_CHECK(esp_wifi_set_country(&pais));

    ESP_ERROR_CHECK(esp_wifi_set_storage(WIFI_STORAGE_RAM));
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_AP));

    wifi_config_t ap_cfg = {
        .ap = {
            .ssid = AP_SSID,
            .ssid_len = strlen(AP_SSID),
            .channel = AP_CANAL,
            .password = AP_SENHA,
            .max_connection = AP_MAX_CONEXOES,
            .authmode = WIFI_AUTH_WPA2_PSK,
            .beacon_interval = 100,
            .pmf_cfg = { .required = false },
        },
    };
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_AP, &ap_cfg));
    ESP_ERROR_CHECK(esp_wifi_set_protocol(WIFI_IF_AP,
                    WIFI_PROTOCOL_11B | WIFI_PROTOCOL_11G | WIFI_PROTOCOL_11N));

    ESP_ERROR_CHECK(esp_wifi_start());

    /* Parametros fixos para CSI consistente entre pacotes */
    ESP_ERROR_CHECK(esp_wifi_set_bandwidth(WIFI_IF_AP, WIFI_BW_HT20));
    ESP_ERROR_CHECK(esp_wifi_set_ps(WIFI_PS_NONE));
    ESP_ERROR_CHECK(esp_wifi_set_max_tx_power(POTENCIA_TX_QDBM));

#if USAR_TAXA_FIXA
    /* Todos os frames de dados em HT MCS0 (OFDM 802.11n) -> CSI com HT-LTF */
    esp_err_t err = esp_wifi_internal_set_fix_rate(WIFI_IF_AP, true, WIFI_PHY_RATE_MCS0_LGI);
    if (err != ESP_OK) {
        ESP_LOGW(TAG, "Nao foi possivel fixar taxa PHY (%s)", esp_err_to_name(err));
    }
#endif

    int8_t pot = 0;
    esp_wifi_get_max_tx_power(&pot);
    ESP_LOGI(TAG, "AP ativo: SSID=%s canal=%d HT20 potencia=%.2f dBm",
             AP_SSID, AP_CANAL, pot * 0.25f);
}

/* --------------------- Timer -> tarefa de envio ----------------- */
static void callback_timer(void *arg)
{
    /* esp_timer roda em contexto de tarefa: so acorda a tarefa de envio */
    if (s_tarefa_envio) {
        xTaskNotifyGive(s_tarefa_envio);
    }
}

static void tarefa_envio(void *arg)
{
    int sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_IP);
    if (sock < 0) {
        ESP_LOGE(TAG, "Falha ao criar socket UDP");
        vTaskDelete(NULL);
        return;
    }

    struct sockaddr_in destino = {
        .sin_family = AF_INET,
        .sin_port = htons(PORTA_UDP_DESTINO),
    };

    pacote_tx_t pkt;
    memset(&pkt, 0, sizeof(pkt));
    pkt.magic = PAYLOAD_MAGIC;

    uint32_t seq = 0, enviados = 0, falhas = 0;
    int64_t t_ultimo_log = esp_timer_get_time();

    while (1) {
        ulTaskNotifyTake(pdTRUE, portMAX_DELAY);

        uint32_t ip = s_ip_sta;
        if (ip == 0) {
            continue;                      /* RX ainda nao conectada */
        }
        destino.sin_addr.s_addr = ip;

        pkt.seq = seq++;
        pkt.t_us = esp_timer_get_time();

        int r = sendto(sock, &pkt, sizeof(pkt), 0,
                       (struct sockaddr *)&destino, sizeof(destino));
        if (r < 0) {
            falhas++;                      /* ENOMEM = fila de TX cheia */
        } else {
            enviados++;
        }

        int64_t agora = esp_timer_get_time();
        if (agora - t_ultimo_log >= (int64_t)INTERVALO_LOG_S * 1000000) {
            float dt = (agora - t_ultimo_log) / 1e6f;
            ESP_LOGI(TAG, "seq=%" PRIu32 " | taxa real=%.1f Hz | falhas=%" PRIu32,
                     seq, enviados / dt, falhas);
            enviados = 0;
            falhas = 0;
            t_ultimo_log = agora;
        }
    }
}

/* ----------------------------- main ----------------------------- */
void app_main(void)
{
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    iniciar_wifi_ap();

    xTaskCreate(tarefa_envio, "tarefa_envio", 4096, NULL, 5, &s_tarefa_envio);

    const esp_timer_create_args_t args_timer = {
        .callback = &callback_timer,
        .name = "timer_envio",
    };
    esp_timer_handle_t timer;
    ESP_ERROR_CHECK(esp_timer_create(&args_timer, &timer));
    ESP_ERROR_CHECK(esp_timer_start_periodic(timer, 1000000 / TAXA_ENVIO_HZ));

    ESP_LOGI(TAG, "Aguardando conexao da RX...");
}