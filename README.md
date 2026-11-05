# Seismic MCP Server (Streamable HTTP + Docker)

MCP server สำหรับข้อมูลแผ่นดินไหว (USGS, TMD) และงานวิเคราะห์ประกันภัยแบบ parametric ตาม `spec.md`
ต่างจาก spec ตรงที่ transport หลักคือ **Streamable HTTP** และรันใน Docker container ได้
(ยังใช้ stdio ได้ด้วย `--transport stdio`)

## ไฟล์

| ไฟล์ | หน้าที่ |
|---|---|
| `seismic_mcp_server.py` | ตัว server ทั้งหมด: JSON-RPC dispatcher, 5 tools, transport HTTP และ stdio |
| `Dockerfile` / `docker-compose.yml` | build และรันใน container (python:3.12-slim, ผู้ใช้ non-root, มี healthcheck) |
| `requirements.txt` | httpx, jsonschema, starlette, uvicorn, tzdata |
| `test_client.py` | smoke test ผ่าน HTTP ใช้แค่ standard library |

## รันด้วย Docker

```bash
docker compose up -d --build
```

```bash
curl http://localhost:8000/healthz
```

```bash
python3 test_client.py
```

MCP endpoint อยู่ที่ `http://localhost:8000/mcp`

แบบไม่ใช้ compose:

```bash
docker build -t seismic-mcp-server:0.1.0 .
```

```bash
docker run -d --name seismic-mcp -p 8000:8000 seismic-mcp-server:0.1.0
```

รันนอก Docker (ต้องใช้ Python 3.11 ขึ้นไป):

```bash
pip install -r requirements.txt && python seismic_mcp_server.py --port 8000
```

## เชื่อมต่อกับ client

**Claude Code**

```bash
claude mcp add --transport http seismic http://localhost:8000/mcp
```

**Claude Desktop** (config รองรับแค่ stdio จึงต้องใช้ `mcp-remote` เป็นสะพาน)

```jsonc
{
  "mcpServers": {
    "seismic-mcp": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "http://localhost:8000/mcp"]
    }
  }
}
```

หรือรันแบบ stdio ใน container เลย:

```jsonc
{
  "mcpServers": {
    "seismic-mcp": {
      "command": "docker",
      "args": ["run", "--rm", "-i", "seismic-mcp-server:0.1.0", "python", "seismic_mcp_server.py", "--transport", "stdio"]
    }
  }
}
```

**MCP Inspector**: `npx @modelcontextprotocol/inspector` แล้วเลือก Transport = *Streamable HTTP* และใส่ URL `http://localhost:8000/mcp`

## การทำงานของ Streamable HTTP

- `POST /mcp` รับ JSON-RPC ทีละข้อความหรือเป็น batch
  - ตอบเป็น `application/json` หาก client รับเฉพาะ `text/event-stream` จะตอบเป็น SSE event เดียว
  - ถ้าเป็น notification จะตอบ `202 Accepted`
- `initialize` จะได้ header `Mcp-Session-Id` กลับไป request ถัด ๆ ไปต้องส่ง header นี้มาด้วย
  - ไม่ส่งจะได้ `400` ส่ง session ที่ไม่รู้จักหรือหมดอายุจะได้ `404`
- `DELETE /mcp` พร้อม `Mcp-Session-Id` เพื่อปิด session
- `GET /mcp` ตอบ `405` เพราะ server นี้ไม่มีข้อความที่ส่งไปหา client เอง (spec อนุญาต)
- ตรวจ header `MCP-Protocol-Version` รองรับ `2025-11-25`, `2025-06-18`, `2025-03-26`, `2024-11-05`
- ตรวจ header `Origin` เพื่อกัน DNS rebinding อนุญาต localhost เป็นค่าเริ่มต้น

## ตัวแปร environment

| ตัวแปร | ค่าเริ่มต้น | ความหมาย |
|---|---|---|
| `MCP_TRANSPORT` | `http` | `http` หรือ `stdio` |
| `MCP_HOST` / `MCP_PORT` / `MCP_PATH` | `0.0.0.0` (ใน Docker) / `8000` / `/mcp` | ที่อยู่ที่ server listen |
| `MCP_AUTH_TOKEN` | ไม่ตั้ง | ถ้าตั้ง ทุก request ต้องส่ง `Authorization: Bearer <token>` |
| `MCP_ALLOWED_ORIGINS` | ไม่ตั้ง | browser origin ที่อนุญาตเพิ่มเติม คั่นด้วย comma (ใส่ `*` = อนุญาตทั้งหมด) |
| `MCP_STATELESS` | `false` | `true` = ไม่ออกและไม่ตรวจ session id |
| `MCP_SESSION_TTL` | `3600` | session ที่ไม่ได้ใช้นานเกินกี่วินาทีจะถูกลบ |
| `TZ` | `Asia/Bangkok` (ใน Docker) | ใช้หา "วันนี้" ของ tool ฝั่ง TMD |
| `UPSTREAM_TIMEOUT` | `20` | timeout (วินาที) ตอนเรียก USGS/TMD |
| `TMD_CACHE_SECONDS` | `60` | cache ผลจาก TMD |
| `LOG_LEVEL` | `info` | log ทั้งหมดออกที่ stderr |

ถ้าจะเปิด server ให้เครื่องอื่นเข้าถึง ควรตั้ง `MCP_AUTH_TOKEN` และวางไว้หลัง reverse proxy ที่เป็น HTTPS

## Tools

| Tool | คำอธิบาย |
|---|---|
| `usgs_search_earthquakes` | ค้นหาแผ่นดินไหวจาก USGS FDSN (ช่วงวันที่, magnitude, ค้นตามรัศมี, limit) |
| `tmd_get_daily_earthquakes` | แผ่นดินไหวของวันที่ระบุจาก TMD (ไม่ระบุ = วันนี้) |
| `tmd_get_recent_earthquakes` | แผ่นดินไหวจาก TMD ย้อนหลัง N วัน (1–7) ตัด event ซ้ำ เรียงจากใหม่ไปเก่า |
| `match_events_to_assets` | จับคู่ event กับทรัพย์สินที่เอาประกันตามระยะทาง (haversine) และ magnitude |
| `compute_parametric_payout` | คำนวณ payout ตาม payout curve (ค่าเริ่มต้น ≤50 km 50%, ≤100 km 25%, ≤200 km 10%) |

ผลลัพธ์ของ `tools/call` ส่งกลับมาสองแบบพร้อมกัน คือ `structuredContent` (object ตาม spec)
และ `content[0].text` (JSON string ของ object เดียวกัน) ส่วน input ที่ผิด schema, HTTP error
และ parse error จะตอบเป็น JSON-RPC `error`:

| code | ความหมาย |
|---|---|
| `-32602` | argument ไม่ถูกต้อง หรือไม่รู้จักชื่อ tool |
| `-32601` | ไม่รู้จัก method |
| `-32700` | JSON parse error |
| `-32001` | USGS/TMD ติดต่อไม่ได้หรือตอบ HTTP error |
| `-32002` | response จาก upstream parse ไม่ได้ |

## ข้อแตกต่างระหว่าง TMD API จริงกับ spec (ตรวจเมื่อ 2026-11-05)

API จริงไม่ตรงกับตัวอย่างใน spec server จึงรองรับทั้งสองรูปแบบ:

- array ชื่อ `DailyEarthquakes` ไม่ใช่ `DailySeismicEvent`
- ไม่มี `EventID` server จึงสร้าง id จากเวลา UTC และพิกัด เช่น `TMD-20261105T095932Z-22.174-100.96`
- ไม่มี `DateTime`/`Location`/`DepthKm` แต่มี `DateTimeUTC`, `DateTimeThai`, `TitleThai`/`OriginThai`, `Depth` และตัวเลขทุกตัวเป็น string
- **ส่ง `Date` ไปแล้วไม่มีผล** API ตอบกลับทุก event ตั้งแต่ 1 ม.ค. ของปีปัจจุบัน (~1,300 รายการ)
  server จึงกรองตามวันที่ (เวลาไทย) เอง และยังส่ง `Date` ตาม spec เผื่อ API แก้ในอนาคต
  ผลคือดูวันที่ก่อน 1 ม.ค. ของปีปัจจุบันไม่ได้
- `time` ของ TMD จะเป็นเวลาไทยพร้อม offset (`2026-11-05T16:59:32+07:00`) ส่วนของ USGS เป็น UTC (`...Z`)

## ส่วนที่ทำเกินจาก spec เล็กน้อย

- `payout_curve` จะถูกเรียงตาม `max_distance_km` จากน้อยไปมากก่อนเลือก band แรกที่เข้าเงื่อนไข
- `payout_percent` ต้องอยู่ระหว่าง 0–1
- radius search ของ USGS ต้องส่ง lat/lon/radius ครบทั้งสามค่า
- `match_events_to_assets` มี `summary` เพิ่ม และ `compute_parametric_payout` มี `magnitude_triggered` เพิ่ม
- `capabilities.tools` มี `listChanged: false` เพิ่มจาก `list`/`call` ตามที่ spec กำหนด
