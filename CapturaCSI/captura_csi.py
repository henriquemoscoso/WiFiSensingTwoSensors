# %% [markdown]
# Captura do CSI da esp_rx (serial) -> HDF5
#
# Uso:
#   python captura_csi_h5.py <nome_sessao>
#   ex.: python captura_csi_h5.py vazio_01
#
# Roda sozinho, sem interacao, ate ser interrompido (Ctrl+C ou botao de parar).
# A interrupcao fecha o arquivo com seguranca. O numero real de pessoas
# e associado depois, na etapa de processamento.
#
# IMPORTANTE: feche o monitor serial do Espressif-IDE antes (a COM so abre em um programa).

# %% Configuracao
import sys
import os
import time
import threading
import queue
import datetime

import numpy as np
import h5py
import serial

PORTAS_CANDIDATAS = ["COM5", "COM6"]   # testadas nessa ordem; a que mandar CSI da RX e usada
BAUD = 921600
PASTA = r"C:\Users\henri\OneDrive\Desktop\WiFiSensingTwoSensors\dados"
SESSAO = "teste_01"       # sobrescrito pelo 1o argumento da linha de comando
DESCRICAO = ""            # texto livre: layout, posicao das placas, observacoes

N_BYTES_CSI = 128         # L-LTF: 64 subportadoras x [imag, real]
LOTE = 100                # amostras por escrita no disco (~1 s a 100 Hz)
INTERVALO_STATUS_S = 5

# ignora argumentos injetados pelo Jupyter
args = [a for a in sys.argv[1:] if not a.startswith("-")]
if len(args) >= 1:
    SESSAO = args[0]

os.makedirs(PASTA, exist_ok=True)
inicio = datetime.datetime.now()
ARQUIVO = os.path.join(PASTA, f"{SESSAO}_{inicio:%Y%m%d_%H%M%S}.h5")

# %% Funcoes auxiliares
def criar_arquivo(caminho):
    f = h5py.File(caminho, "w")
    kw = dict(maxshape=(None,), chunks=(4096,))
    f.create_dataset("csi", shape=(0, N_BYTES_CSI), maxshape=(None, N_BYTES_CSI),
                     dtype="int8", chunks=(1024, N_BYTES_CSI), compression="gzip", compression_opts=4)
    f.create_dataset("seq", shape=(0,), dtype="uint32", **kw)
    f.create_dataset("t_us", shape=(0,), dtype="int64", **kw)      # relogio da ESP32 (us)
    f.create_dataset("rssi", shape=(0,), dtype="int8", **kw)
    f.create_dataset("t_host", shape=(0,), dtype="float64", **kw)  # relogio do PC (unix, s)

    txt = h5py.string_dtype("utf-8")

    g = f.create_group("log")                                      # linhas '#' do firmware
    g.create_dataset("t_host", shape=(0,), dtype="float64", maxshape=(None,))
    g.create_dataset("texto", shape=(0,), dtype=txt, maxshape=(None,))

    f.attrs["sessao"] = SESSAO
    f.attrs["descricao"] = DESCRICAO
    f.attrs["inicio"] = inicio.isoformat()
    f.attrs["porta"] = PORTA
    f.attrs["baud"] = BAUD
    f.attrs["formato_csi"] = ("int8 brutos; 64 subportadoras L-LTF; pares [imag, real] "
                              "(coluna 2k = imag_k, 2k+1 = real_k); 4 primeiros bytes zerados "
                              "quando first_word_invalid")
    f.attrs["origem"] = "esp_rx (ESP-IDF) -> linhas CSI_US,<seq>,<t_us>,<rssi>,<len>,<valores>"
    return f


def abrir_serial(porta, baud, timeout=1):
    """Abre a porta SEM mexer em DTR/RTS (evita resetar a ESP32 ao abrir)."""
    ser = serial.Serial()
    ser.port = porta
    ser.baudrate = baud
    ser.timeout = timeout
    ser.dtr = False
    ser.rts = False
    ser.open()
    return ser


def detectar_porta(candidatas, baud, espera_s=6):
    """Testa cada porta e devolve a primeira que estiver enviando dados da esp_rx.

    Criterio: aparecer uma linha 'CSI_US,' (CSI chegando) ou '# STATS' (RX viva,
    mesmo que ainda sem CSI). Uma porta que so abre nao basta: o TX tambem pode
    estar plugado e mandaria apenas os logs dele.
    """
    for porta in candidatas:
        try:
            ser = abrir_serial(porta, baud, timeout=0.5)
        except serial.SerialException as e:
            print(f"  {porta}: nao abriu ({e.__class__.__name__}) - ausente ou em uso pelo monitor do IDE")
            continue

        print(f"  {porta}: aberta, procurando dados da RX por ate {espera_s} s...")
        ser.reset_input_buffer()
        t_lim = time.time() + espera_s
        achou = None
        while time.time() < t_lim:
            linha = ser.readline().decode("ascii", errors="replace")
            if linha.startswith("CSI_US,"):
                achou = "CSI chegando"
                break
            if linha.startswith("# STATS"):
                achou = "RX ativa (# STATS)"
                break
        ser.close()

        if achou:
            print(f"  {porta}: RX encontrada ({achou})")
            return porta
        print(f"  {porta}: sem dados da RX")

    raise RuntimeError(
        f"Nenhuma RX encontrada em {candidatas}. Verifique: placa plugada, monitor do IDE "
        f"fechado, baud {baud} igual ao do firmware, TX ligado."
    )


def anexar(ds, dados):
    n = ds.shape[0]
    ds.resize(n + len(dados), axis=0)
    ds[n:] = dados


def leitor_serial(ser, fila, parar):
    """Thread: so le linhas da serial e enfileira (parse fica no loop principal)."""
    while not parar.is_set():
        try:
            linha = ser.readline()
        except serial.SerialException as e:
            fila.put(("erro", time.time(), str(e)))
            break
        if linha:
            fila.put(("linha", time.time(), linha))


# %% Captura
print("Procurando a RX nas portas:", ", ".join(PORTAS_CANDIDATAS))
PORTA = detectar_porta(PORTAS_CANDIDATAS, BAUD)

f = criar_arquivo(ARQUIVO)
ser = abrir_serial(PORTA, BAUD, timeout=1)
ser.reset_input_buffer()

fila = queue.Queue()
parar = threading.Event()
threading.Thread(target=leitor_serial, args=(ser, fila, parar), daemon=True).start()

buf_csi, buf_seq, buf_tus, buf_rssi, buf_th = [], [], [], [], []
total = 0
descartadas = 0
saltos_seq = 0
ultimo_seq = None
t0 = time.time()
t_status = t0
total_status = 0


def descarregar():
    global buf_csi, buf_seq, buf_tus, buf_rssi, buf_th
    if not buf_seq:
        return
    anexar(f["csi"], np.array(buf_csi, dtype=np.int8))
    anexar(f["seq"], np.array(buf_seq, dtype=np.uint32))
    anexar(f["t_us"], np.array(buf_tus, dtype=np.int64))
    anexar(f["rssi"], np.array(buf_rssi, dtype=np.int8))
    anexar(f["t_host"], np.array(buf_th, dtype=np.float64))
    f.flush()
    buf_csi, buf_seq, buf_tus, buf_rssi, buf_th = [], [], [], [], []


print(f"Gravando em: {ARQUIVO}")
print("Capturando... (Ctrl+C para encerrar a sessao)\n")

try:
    while True:
        try:
            tipo, t_host, conteudo = fila.get(timeout=0.5)
        except queue.Empty:
            continue

        if tipo == "erro":
            print(f"!!! Erro na serial: {conteudo}")
            break

        linha = conteudo.decode("ascii", errors="replace").strip()

        if linha.startswith("CSI_US,"):
            p = linha.split(",")
            try:
                seq, t_us, rssi, n = int(p[1]), int(p[2]), int(p[3]), int(p[4])
                valores = [int(v) for v in p[5:5 + n]]
            except (ValueError, IndexError):
                descartadas += 1
                continue
            if n != N_BYTES_CSI or len(valores) != N_BYTES_CSI:
                descartadas += 1
                continue
            if ultimo_seq is not None and seq > ultimo_seq + 1:
                saltos_seq += seq - ultimo_seq - 1
            ultimo_seq = seq

            buf_csi.append(valores)
            buf_seq.append(seq)
            buf_tus.append(t_us)
            buf_rssi.append(rssi)
            buf_th.append(t_host)
            total += 1
            if len(buf_seq) >= LOTE:
                descarregar()

        elif linha.startswith("#"):
            anexar(f["log/t_host"], [t_host])
            anexar(f["log/texto"], [linha])
            if not linha.startswith("# STATS"):
                print(linha)                     # mostra banners/erros do firmware

        agora = time.time()
        if agora - t_status >= INTERVALO_STATUS_S:
            taxa = (total - total_status) / (agora - t_status)
            mb = os.path.getsize(ARQUIVO) / 1e6
            print(f"[{agora - t0:7.1f} s] amostras={total}  taxa={taxa:5.1f} Hz  "
                  f"saltos_seq={saltos_seq}  descartadas={descartadas}  arquivo={mb:.1f} MB")
            t_status, total_status = agora, total

except KeyboardInterrupt:
    print("\nEncerrando...")

finally:
    parar.set()
    descarregar()
    f.attrs["fim"] = datetime.datetime.now().isoformat()
    f.attrs["total_amostras"] = total
    f.attrs["saltos_seq"] = saltos_seq
    f.attrs["linhas_descartadas"] = descartadas
    f.close()
    ser.close()
    dur = time.time() - t0
    print(f"\nArquivo fechado: {ARQUIVO}")
    print(f"{total} amostras em {dur:.1f} s ({total / max(dur, 1e-9):.1f} Hz medio), "
          f"saltos_seq={saltos_seq}, descartadas={descartadas}")
