import os
from datetime import datetime
import httpx
import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# อ่านลิงก์ฐานข้อมูลจาก Cloud (หากรันในเครื่องจะใช้ localhost อัตโนมัติ)
DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:123456789@localhost:5432/uttaradit_pm25",
)

app = FastAPI(title="Uttaradit PM2.5 Safe-Route Navigation API")
scheduler = AsyncIOScheduler()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

UTTARADIT_DISTRICTS = [
    {"name": "อ.เมืองอุตรดิตถ์", "lat": 17.6256, "lng": 100.0985, "pm25": 35.0},
    {"name": "อ.ลับแล", "lat": 17.6531, "lng": 100.0369, "pm25": 32.0},
    {"name": "อ.ตรอน", "lat": 17.4792, "lng": 100.1292, "pm25": 34.0},
    {"name": "อ.ทองแสนขัน", "lat": 17.5144, "lng": 100.3414, "pm25": 30.0},
    {"name": "อ.ท่าปลา", "lat": 17.7819, "lng": 100.3125, "pm25": 28.0},
    {"name": "อ.น้ำปาด", "lat": 17.7350, "lng": 100.6836, "pm25": 26.0},
    {"name": "อ.ฟากท่า", "lat": 18.0069, "lng": 100.8694, "pm25": 25.0},
    {"name": "อ.บ้านโคก", "lat": 18.1444, "lng": 101.0736, "pm25": 24.0},
    {"name": "อ.พิชัย", "lat": 17.2861, "lng": 100.0883, "pm25": 36.0},
]

LAST_UPDATED = "ยังไม่ได้อัปเดต"


async def update_realtime_pm25():
    """ดึงค่าฝุ่น PM2.5 เรียลไทม์ทั้ง 9 อำเภอ และอัปเดตลงถนนทุกเส้น"""
    global LAST_UPDATED
    lats = ",".join(str(d["lat"]) for d in UTTARADIT_DISTRICTS)
    lngs = ",".join(str(d["lng"]) for d in UTTARADIT_DISTRICTS)
    url = f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude={lats}&longitude={lngs}&current=pm2_5"

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.get(url)
            res.raise_for_status()
            data = res.json()

            if isinstance(data, list):
                for idx, item in enumerate(data):
                    val = item.get("current", {}).get("pm2_5")
                    if val is not None:
                        UTTARADIT_DISTRICTS[idx]["pm25"] = float(val)
            elif isinstance(data, dict):
                val = data.get("current", {}).get("pm2_5")
                if val is not None:
                    UTTARADIT_DISTRICTS[0]["pm25"] = float(val)
    except Exception as e:
        print(f"[Warning] ดึงข้อมูลฝุ่นออนไลน์ไม่สำเร็จ ใช้ค่าสำรองล่าสุด: {e}")

    values_sql = ", ".join(
        f"('{d['name']}', ST_SetSRID(ST_Point({d['lng']}, {d['lat']}), 4326), {d['pm25']})"
        for d in UTTARADIT_DISTRICTS
    )

    update_query = f"""
    WITH stations(name, geom, pm25) AS (
        VALUES {values_sql}
    )
    UPDATE roads r
    SET pm25 = ROUND((
        (
            SELECT SUM(s.pm25 / POWER(GREATEST(ST_Distance(ST_Centroid(r.geom), s.geom), 0.005), 2))
                 / SUM(1.0 / POWER(GREATEST(ST_Distance(ST_Centroid(r.geom), s.geom), 0.005), 2))
            FROM stations s
        ) * CASE r.highway
            WHEN 'trunk' THEN 1.25
            WHEN 'trunk_link' THEN 1.20
            WHEN 'primary' THEN 1.18
            WHEN 'primary_link' THEN 1.15
            WHEN 'secondary' THEN 1.10
            WHEN 'tertiary' THEN 1.02
            WHEN 'residential' THEN 0.88
            WHEN 'living_street' THEN 0.82
            ELSE 0.95
        END
    )::numeric, 1);
    """

    try:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(update_query)
            conn.commit()
        LAST_UPDATED = datetime.now().strftime("%H:%M น. (%d/%m/%Y)")
        print(f"[OK] อัปเดตค่าฝุ่นลงโครงข่ายถนนสำเร็จ เวลา {LAST_UPDATED}")
    except Exception as e:
        print(f"[Error] อัปเดตฐานข้อมูลไม่สำเร็จ: {e}")


@app.on_event("startup")
async def startup_event():
    await update_realtime_pm25()
    # ตั้งเวลาอัปเดตค่าฝุ่นใหม่อัตโนมัติทุก ๆ 1 ชั่วโมง
    scheduler.add_job(update_realtime_pm25, "interval", hours=1)
    scheduler.start()


@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown(wait=False)


def compute_single_route(
    cur,
    start_node: int,
    end_node: int,
    alpha: float,
    directed: bool,
    mode: str,
    mask_factor: float,
):
    weight_factor = round(float(alpha) * 2.5, 4)
    edge_sql = (
        f"SELECT id, source, target, "
        f"base_time_sec * (1.0 + {weight_factor} * (pm25 / 25.0)) AS cost, "
        f"CASE WHEN oneway THEN -1 ELSE base_time_sec * (1.0 + {weight_factor} * (pm25 / 25.0)) END AS reverse_cost "
        f"FROM roads"
    )

    sql = """
    SELECT
        d.seq,
        d.node,
        r.id AS road_id,
        r.name,
        r.length_m,
        r.speed_kmh,
        r.pm25,
        r.source,
        r.target,
        CASE
            WHEN d.node = r.source THEN ST_AsGeoJSON(r.geom)::json
            ELSE ST_AsGeoJSON(ST_Reverse(r.geom))::json
        END AS geojson_geom
    FROM pgr_dijkstra(%s, %s, %s, directed := %s) d
    JOIN roads r ON d.edge = r.id
    ORDER BY d.seq;
    """
    cur.execute(sql, (edge_sql, start_node, end_node, directed))
    edges = cur.fetchall()

    if not edges:
        return None

    mode_exposure_mult = {
        "walk": 1.35,
        "bike": 1.45,
        "motorcycle": 1.20,
        "car": 0.65,
    }.get(mode, 1.0)

    total_length_m = 0.0
    total_time_sec = 0.0
    weighted_pm_sum = 0.0
    merged_coords = []
    road_names = []

    for edge in edges:
        length_m = float(edge["length_m"])
        pm25 = float(edge["pm25"])
        road_speed = float(edge["speed_kmh"])

        if mode == "walk":
            eff_speed = 5.0
        elif mode == "bike":
            eff_speed = 15.0
        elif mode == "motorcycle":
            eff_speed = max(20.0, road_speed * 0.95)
        else:
            eff_speed = road_speed

        seg_sec = length_m / (eff_speed * 1000.0 / 3600.0)
        total_length_m += length_m
        total_time_sec += seg_sec
        weighted_pm_sum += pm25 * length_m

        coords = edge["geojson_geom"]["coordinates"]
        if merged_coords and merged_coords[-1] == coords[0]:
            merged_coords.extend(coords[1:])
        else:
            merged_coords.extend(coords)

        rname = edge["name"]
        if rname and rname != "ถนน/ซอยไม่มีชื่อ" and (not road_names or road_names[-1] != rname):
            road_names.append(rname)

    avg_ambient_pm25 = weighted_pm_sum / max(1.0, total_length_m)
    effective_pm25 = round(avg_ambient_pm25 * mode_exposure_mult * mask_factor, 1)

    return {
        "distance_km": round(total_length_m / 1000.0, 2),
        "duration_min": max(1, round(total_time_sec / 60.0)),
        "ambient_pm25": round(avg_ambient_pm25, 1),
        "effective_pm25": effective_pm25,
        "road_summary": " → ".join(road_names[:4]) if road_names else "เส้นทางย่อยในพื้นที่",
        "geometry": {
            "type": "LineString",
            "coordinates": merged_coords,
        },
    }


@app.get("/")
def serve_frontend():
    return FileResponse("index.html")


@app.get("/api/status")
def get_air_status():
    mueang_pm = UTTARADIT_DISTRICTS[0]["pm25"]
    return {
        "last_updated": LAST_UPDATED,
        "mueang_pm25": round(mueang_pm, 1),
        "districts": UTTARADIT_DISTRICTS,
    }


@app.get("/api/hotspots")
def get_hotspots():
    sql = """
    SELECT
        ROUND(ST_X(ST_Centroid(geom))::numeric, 4) AS lng,
        ROUND(ST_Y(ST_Centroid(geom))::numeric, 4) AS lat,
        ROUND(AVG(pm25)::numeric, 1) AS pm25
    FROM roads
    GROUP BY 1, 2
    LIMIT 1500;
    """
    with psycopg.connect(DB_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

    features = [
        {
            "type": "Feature",
            "properties": {"pm25": float(r["pm25"])},
            "geometry": {"type": "Point", "coordinates": [float(r["lng"]), float(r["lat"])]},
        }
        for r in rows
    ]
    return {"type": "FeatureCollection", "features": features}


@app.get("/api/route")
def calculate_route(
    orig_lng: float = Query(...),
    orig_lat: float = Query(...),
    dest_lng: float = Query(...),
    dest_lat: float = Query(...),
    alpha: float = Query(0.5, ge=0.0, le=1.0),
    mode: str = Query("motorcycle"),
    mask: float = Query(1.0, ge=0.05, le=1.0),
):
    directed = mode in ("car", "motorcycle")

    nearest_node_sql = """
    SELECT n.id
    FROM nodes n
    JOIN roads r ON (r.source = n.id OR r.target = n.id)
    WHERE r.highway NOT IN ('service')
    ORDER BY n.geom <-> ST_SetSRID(ST_Point(%s, %s), 4326)
    LIMIT 1;
    """

    with psycopg.connect(DB_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(nearest_node_sql, (orig_lng, orig_lat))
            start_row = cur.fetchone()

            cur.execute(nearest_node_sql, (dest_lng, dest_lat))
            end_row = cur.fetchone()

            if not start_row or not end_row:
                raise HTTPException(status_code=404, detail="ไม่พบจุดเชื่อมต่อถนนใกล้พิกัดที่เลือก")

            start_id = start_row["id"]
            end_id = end_row["id"]

            fast_route = compute_single_route(cur, start_id, end_id, 0.0, directed, mode, mask)
            safe_alpha = alpha if alpha > 0 else 0.5
            safe_route = compute_single_route(cur, start_id, end_id, safe_alpha, directed, mode, mask)

            if not fast_route or not safe_route:
                raise HTTPException(status_code=400, detail="ไม่สามารถคำนวณเส้นทางเชื่อมถึงกันได้ กรุณาลองขยับหมุด")

    reduction_pct = 0
    if fast_route["ambient_pm25"] > 0:
        diff = fast_route["ambient_pm25"] - safe_route["ambient_pm25"]
        reduction_pct = max(0, round((diff / fast_route["ambient_pm25"]) * 100))

    return {
        "fast_route": fast_route,
        "safe_route": safe_route,
        "reduction_pct": reduction_pct,
        "last_updated": LAST_UPDATED,
    }