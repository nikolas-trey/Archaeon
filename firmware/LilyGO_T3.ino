#include <Wire.h>
#include <SPI.h>
#include <LoRa.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>
#include <WiFi.h>
#include <Preferences.h>

#define SCREEN_WIDTH 128
#define SCREEN_HEIGHT 64
#define OLED_RESET    -1

Adafruit_SSD1306 display(SCREEN_WIDTH, SCREEN_HEIGHT, &Wire, OLED_RESET);
const uint8_t OLED_ADDRESS = 0x3C;

#define LORA_SCK     5
#define LORA_MISO    19
#define LORA_MOSI    27
#define LORA_SS      18
#define LORA_RST     23
#define LORA_DIO0    26

#define LED_PIN      25

#define LORA_BAND    868E6

const size_t MAX_SEND_LEN = 200;

const unsigned long LED_ON_MS = 100;

unsigned long lastSendTime = 0;
unsigned long ledOffTime = 0;
unsigned long sentCount = 0;
unsigned long recvCount = 0;
int lastRSSI = 0;

#define OUT_QUEUE_SIZE 20
static char outQueue[OUT_QUEUE_SIZE][MAX_SEND_LEN + 1];
static uint8_t qHead = 0;
static uint8_t qTail = 0;
static uint8_t qCount = 0;

#define SERIAL_BUF_LEN 512
static char serialBuf[SERIAL_BUF_LEN];
static size_t serialBufIdx = 0;

static char lastSentBuf[MAX_SEND_LEN + 1] = "<none>";
static char lastReceivedBuf[256] = "<none>";

unsigned long lastOledUpdate = 0;
const unsigned long OLED_UPDATE_MS = 300;
bool oledDirty = true;

const unsigned long SEND_INTERVAL_MS = 100;

#define AP_PASSWORD ""

static char apSSID[20];
static char nodeID[12]; 

Preferences prefs;

void initNodeID() {
  prefs.begin("archaeon", false); 

  if (prefs.isKey("node_id")) {

    String stored = prefs.getString("node_id", "");
    stored.toCharArray(nodeID, sizeof(nodeID));
    Serial.printf("Loaded persistent node ID: %s\n", nodeID);
  } else {

    uint32_t rnd = esp_random(); 

    snprintf(nodeID, sizeof(nodeID), "node_%06lx", (unsigned long)(rnd & 0x00FFFFFFu));
    prefs.putString("node_id", nodeID);
    Serial.printf("Generated new node ID: %s\n", nodeID);
  }

  prefs.end();

  snprintf(apSSID, sizeof(apSSID), "%s", nodeID);
}

uint32_t crc32_bytes(const uint8_t *data, size_t len) {
  uint32_t crc = 0xFFFFFFFFu;
  for (size_t i = 0; i < len; ++i) {
    crc ^= data[i];
    for (int b = 0; b < 8; ++b) {
      if (crc & 1u) crc = (crc >> 1) ^ 0xEDB88320u;
      else          crc = (crc >> 1);
    }
  }
  return ~crc;
}

String crc32HexFromBuf(const char *s) {
  if (!s || s[0] == '\0') return String("<none>");
  size_t len = strlen(s);
  uint32_t c = crc32_bytes((const uint8_t *)s, len);
  char buf[16];
  sprintf(buf, "%08lx", (unsigned long)c);
  return String(buf);
}

String crc32HexPrefixFromBuf(const char *s, int hexChars = 8) {
  String full = crc32HexFromBuf(s);
  if (full == "<none>") return full;
  if (hexChars <= 0) return full;
  if (hexChars >= (int)full.length()) return full;
  return full.substring(0, hexChars);
}

void ledOn() {
  digitalWrite(LED_PIN, HIGH);
  ledOffTime = millis() + LED_ON_MS;
}

void ledOff() {
  digitalWrite(LED_PIN, LOW);
}

void setup() {
  Serial.begin(115200);
  while (!Serial) delay(1);
  Serial.println();
  Serial.println("LilyGO T3 v1.6.1 - Archaeon Node");

  pinMode(LED_PIN, OUTPUT);
  ledOff();

  Wire.begin(21, 22);

  if (!display.begin(SSD1306_SWITCHCAPVCC, OLED_ADDRESS)) {
    Serial.println("SSD1306 init failed");
    while (true) { delay(1000); }
  }

  display.clearDisplay();
  display.setTextWrap(false);

  display.setTextSize(2);
  display.setTextColor(SSD1306_WHITE);

  display.setCursor(16, 6);
  display.print("Archaeon");

  display.setTextSize(1);

  display.setCursor(52, 28);
  display.print("Node");

  display.drawFastHLine(0, 38, SCREEN_WIDTH, SSD1306_WHITE);

  display.setCursor(4, 44);
  display.print("Freq:");
  display.print(LORA_BAND / 1e6, 3);
  display.print(" MHz");

  display.setCursor(4, 54);
  display.print("Initialising...");
  display.display();

  initNodeID();

  SPI.begin(LORA_SCK, LORA_MISO, LORA_MOSI);
  LoRa.setPins(LORA_SS, LORA_RST, LORA_DIO0);

  if (!LoRa.begin(LORA_BAND)) {
    Serial.println("LoRa init failed!");
    display.clearDisplay();
    display.setTextSize(2);
    display.setCursor(0, 20);
    display.print("LoRa FAIL");
    display.display();
    while (true) { delay(1000); }
  }
  Serial.print("LoRa init OK @ ");
  Serial.print(LORA_BAND / 1e6, 3);
  Serial.println(" MHz");

  WiFi.mode(WIFI_AP);
  const char *apPass = (strlen(AP_PASSWORD) >= 8) ? AP_PASSWORD : nullptr;
  WiFi.softAP(apSSID, apPass);

  Serial.printf("Wi-Fi AP started  SSID: %s  Pass: %s  IP: %s\n",
                apSSID,
                apPass ? AP_PASSWORD : "<open>",
                WiFi.softAPIP().toString().c_str());

  qHead = qTail = qCount = 0;
  serialBufIdx = 0;

  strncpy(lastSentBuf,     "<none>", sizeof(lastSentBuf)     - 1);
  strncpy(lastReceivedBuf, "<none>", sizeof(lastReceivedBuf) - 1);

  delay(800);

  oledDirty = true;
}

void updateOLED() {
  unsigned long now = millis();
  if (!oledDirty && (now - lastOledUpdate) < OLED_UPDATE_MS) return;
  lastOledUpdate = now;
  oledDirty = false;

  display.clearDisplay();
  display.setTextSize(1);
  display.setTextWrap(false);
  display.setTextColor(SSD1306_WHITE);

  display.setCursor(0, 0);
  display.print("Archaeon Node");

  {
    char rssiBuf[12];
    snprintf(rssiBuf, sizeof(rssiBuf), "%4ddBm", lastRSSI);
    display.setCursor(86, 0);
    display.print(rssiBuf);
  }

  display.drawFastHLine(0, 10, SCREEN_WIDTH, SSD1306_WHITE);

  display.setCursor(0, 13);
  display.print("Tx:");
  display.print(sentCount);
  display.setCursor(64, 13);
  display.print("Rx:");
  display.print(recvCount);

  display.setCursor(0, 23);
  display.print("Queue: ");
  display.print(qCount);
  display.print(" / ");
  display.print(OUT_QUEUE_SIZE);

  display.drawFastHLine(0, 33, SCREEN_WIDTH, SSD1306_WHITE);

  String sentCRC = crc32HexPrefixFromBuf(lastSentBuf, 8);
  display.setCursor(0, 36);
  display.print("TxCRC: ");
  display.print(sentCRC);

  String recvCRC = crc32HexPrefixFromBuf(lastReceivedBuf, 8);
  display.setCursor(0, 46);
  display.print("RxCRC: ");
  display.print(recvCRC);

  display.drawFastHLine(0, 56, SCREEN_WIDTH, SSD1306_WHITE);

  display.setCursor(0, 57);
  display.print(apSSID);

  display.display();
}

bool enqueueMessage(const char *msg) {
  if (!msg) return false;
  if (qCount >= OUT_QUEUE_SIZE) {
    Serial.println("[WARN] Outbound queue full, dropping message");
    return false;
  }
  strncpy(outQueue[qTail], msg, MAX_SEND_LEN);
  outQueue[qTail][MAX_SEND_LEN] = '\0';
  qTail = (qTail + 1) % OUT_QUEUE_SIZE;
  qCount++;
  oledDirty = true;
  return true;
}

bool dequeueMessage(char *dest, size_t destLen) {
  if (qCount == 0) return false;
  if (!dest || destLen == 0) return false;
  strncpy(dest, outQueue[qHead], destLen - 1);
  dest[destLen - 1] = '\0';
  qHead = (qHead + 1) % OUT_QUEUE_SIZE;
  qCount--;
  oledDirty = true;
  return true;
}

void sendLoRaStringNonBlocking(const char *s) {
  if (!s || s[0] == '\0') return;

  strncpy(lastSentBuf, s, sizeof(lastSentBuf) - 1);
  lastSentBuf[sizeof(lastSentBuf) - 1] = '\0';

  ledOn();

  LoRa.beginPacket();
  LoRa.print(s);
  LoRa.endPacket();

  sentCount++;
  lastSendTime = millis();

  Serial.print("[TX ");
  Serial.print(sentCount);
  Serial.print("] ");
  Serial.println(s);

  oledDirty = true;
}

void loop() {
  unsigned long now = millis();

  if (ledOffTime > 0 && now >= ledOffTime) {
    ledOff();
    ledOffTime = 0;
  }

  while (Serial.available() > 0) {
    char c = (char)Serial.read();

    if (c == '\r' || c == '\n') {
      if (serialBufIdx > 0) {
        serialBuf[serialBufIdx] = '\0';
        if (!enqueueMessage(serialBuf)) {
          Serial.println("[WARN] Message dropped (queue full)");
        }
        serialBufIdx = 0;
      }
    } else {
      if (serialBufIdx < (SERIAL_BUF_LEN - 1)) {
        serialBuf[serialBufIdx++] = c;
      } else {
        serialBuf[SERIAL_BUF_LEN - 1] = '\0';
        if (!enqueueMessage(serialBuf)) {
          Serial.println("[WARN] Message dropped (queue full) after serial overflow");
        }
        serialBufIdx = 0;
      }
    }
  }

  if (qCount > 0 && (now - lastSendTime) >= SEND_INTERVAL_MS) {
    char msg[MAX_SEND_LEN + 1];
    if (dequeueMessage(msg, sizeof(msg))) {
      sendLoRaStringNonBlocking(msg);
      lastSendTime = now;
    }
  }

  int packetSize = LoRa.parsePacket();
  if (packetSize) {
    ledOn();

    size_t idx = 0;
    const size_t RX_BUF_LEN = sizeof(lastReceivedBuf);
    while (LoRa.available() && idx < (RX_BUF_LEN - 1)) {
      lastReceivedBuf[idx++] = (char)LoRa.read();
    }
    lastReceivedBuf[idx] = '\0';

    lastRSSI = LoRa.packetRssi();
    recvCount++;

    Serial.print("[RX ");
    Serial.print(recvCount);
    Serial.print("] ");
    Serial.print(lastReceivedBuf);
    Serial.print("  RSSI=");
    Serial.println(lastRSSI);

    oledDirty = true;
  }

  updateOLED();

  delay(1);
}

