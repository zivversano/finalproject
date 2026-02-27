#!/bin/bash
# ================================================================
#  🚌  Israel Public Transit - Real-Time Monitoring Platform
#  run.sh — קובץ הרצה ראשי
#  הרץ אותו פעם אחת: bash run.sh
#  הוא ידאג לכל השאר אוטומטית.
# ================================================================

set -e  # Stop on first error

# ── צבעים לפלט ────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

# ── פונקציות עזר ──────────────────────────────────────────────
log()     { echo -e "${BOLD}${BLUE}[$(date '+%H:%M:%S')]${NC} $1"; }
success() { echo -e "${GREEN}✅ $1${NC}"; }
warn()    { echo -e "${YELLOW}⚠️  $1${NC}"; }
error()   { echo -e "${RED}❌ $1${NC}"; exit 1; }
step()    { echo -e "\n${BOLD}${CYAN}━━━  שלב $1  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }
wait_for_service() {
  local name=$1 url=$2 max=$3
  log "ממתין ל-$name להיות מוכן..."
  for i in $(seq 1 $max); do
    if curl -sf "$url" > /dev/null 2>&1; then
      success "$name מוכן!"
      return 0
    fi
    echo -n "."
    sleep 3
  done
  error "$name לא נענה תוך $(($max * 3)) שניות"
}

# ─────────────────────────────────────────────────────────────
#  BANNER
# ─────────────────────────────────────────────────────────────
clear
echo -e "${BOLD}${BLUE}"
echo "  ╔══════════════════════════════════════════════════════╗"
echo "  ║    🚌  Israel Public Transit Monitoring Platform     ║"
echo "  ║         מערכת ניטור תחבורה ציבורית בזמן אמת        ║"
echo "  ╚══════════════════════════════════════════════════════╝"
echo -e "${NC}"

# ─────────────────────────────────────────────────────────────
#  שלב 0: בדיקות מקדימות
# ─────────────────────────────────────────────────────────────
step "0 — בדיקות מקדימות"

# Docker
if ! command -v docker &> /dev/null; then
  error "Docker לא מותקן. הורד מ: https://docs.docker.com/get-docker/"
fi
success "Docker מותקן: $(docker --version)"

# Docker Compose
if ! command -v docker compose &> /dev/null; then
  error "Docker Compose לא נמצא."
fi
success "Docker Compose מותקן: $(docker compose version --short)"

# Python 3
if ! command -v python3 &> /dev/null; then
  error "Python3 לא מותקן"
fi
success "Python: $(python3 --version)"

# .env
if [ ! -f ".env" ]; then
  if [ -f ".env.example" ]; then
    warn ".env לא נמצא — מעתיק מ-.env.example"
    cp .env.example .env
    echo ""
    echo -e "${YELLOW}${BOLD}  ⚠️  חשוב: ערוך את קובץ .env לפני המשך!${NC}"
    echo -e "  פתח: ${CYAN}nano .env${NC} או ${CYAN}code .env${NC}"
    echo -e "  מלא את הסיסמאות ומפתחות ה-API."
    echo ""
    read -p "  האם כבר מילאת את .env? (y/n): " confirm
    if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
      warn "פתח .env ומלא את הפרטים, ואז הרץ מחדש."
      exit 0
    fi
  else
    error "לא נמצא .env ולא .env.example"
  fi
fi
success "קובץ .env נמצא"

# בדיקה שהמשתנים הקריטיים קיימים (מינימום לפיתוח מקומי)
source .env 2>/dev/null || true
if [ -z "$USE_MINIO" ]; then
  warn "USE_MINIO לא מוגדר — מגדיר כ-true לפיתוח מקומי"
  echo "USE_MINIO=true" >> .env
fi

# ─────────────────────────────────────────────────────────────
#  שלב 1: התקנת Python Dependencies
# ─────────────────────────────────────────────────────────────
step "1 — התקנת ספריות Python"

if [ ! -d "venv" ]; then
  log "יוצר סביבת Python וירטואלית..."
  python3 -m venv venv
  success "סביבה וירטואלית נוצרה"
fi

log "מפעיל סביבה וירטואלית ומתקין ספריות..."
source venv/bin/activate

# Upgrade pip quietly
pip install --upgrade pip -q

# Install only if a key package is missing (avoids slow re-resolution on every run)
if ! python3 -c "import kafka, pandas, boto3, dotenv, geopy; from google.transit import gtfs_realtime_pb2" 2>/dev/null; then
  pip install -r requirements.txt -q
  success "כל הספריות הותקנו"
else
  success "כל הספריות כבר מותקנות — מדלג"
fi

# ─────────────────────────────────────────────────────────────
#  שלב 2: הרמת Docker Services
# ─────────────────────────────────────────────────────────────
step "2 — הרמת שירותי Docker"

log "מוריד images נדרשים (עשוי לקחת כמה דקות בפעם הראשונה)..."
docker compose pull --quiet 2>/dev/null || true

log "מפעיל את כל השירותים..."
docker compose up -d

success "כל הקונטיינרים הועלו"
echo ""
docker compose ps
echo ""

# ─────────────────────────────────────────────────────────────
#  שלב 3: המתנה לשירותים קריטיים
# ─────────────────────────────────────────────────────────────
step "3 — המתנה לשירותים להיות מוכנים"

wait_for_service "Kafka UI"  "http://localhost:8080"        30
wait_for_service "MinIO"     "http://localhost:9000/minio/health/live" 20
wait_for_service "Airflow"   "http://localhost:8081/health"  60

# ─────────────────────────────────────────────────────────────
#  שלב 4: יצירת Kafka Topics
# ─────────────────────────────────────────────────────────────
step "4 — יצירת Kafka Topics"

log "ממתין ל-Kafka broker להיות מוכן..."
sleep 5

# בודק אם topics כבר קיימים
EXISTING=$(docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null || echo "")

for topic in "bus-positions" "train-positions" "trip-updates" "service-alerts" "delay-events" "pipeline-errors"; do
  if echo "$EXISTING" | grep -q "^${topic}$"; then
    warn "Topic '${topic}' כבר קיים — מדלג"
  else
    docker exec kafka kafka-topics \
      --create --if-not-exists \
      --bootstrap-server localhost:9092 \
      --partitions 4 \
      --replication-factor 1 \
      --topic "$topic" 2>/dev/null
    success "Topic נוצר: $topic"
  fi
done

# ─────────────────────────────────────────────────────────────
#  שלב 5: אתחול Airflow
# ─────────────────────────────────────────────────────────────
step "5 — אתחול Apache Airflow"

log "מאתחל מסד נתוני Airflow..."
docker compose exec -T airflow-webserver airflow db init 2>/dev/null || \
docker compose exec -T airflow-webserver airflow db migrate 2>/dev/null || \
warn "Airflow DB כבר מאותחל"

log "יוצר משתמש admin ל-Airflow..."
docker compose exec -T airflow-webserver airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@transit.il 2>/dev/null || warn "משתמש admin כבר קיים"

success "Airflow מוכן"

# ─────────────────────────────────────────────────────────────
#  שלב 6: אתחול MinIO Bucket
# ─────────────────────────────────────────────────────────────
step "6 — אתחול MinIO (Data Lake מקומי)"

log "יוצר bucket לנתוני תחבורה..."
python3 - <<'EOF'
import sys
sys.path.insert(0, '.')
try:
    import boto3
    from botocore.exceptions import ClientError
    client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin123',
        region_name='us-east-1'
    )
    bucket = 'israel-transit-lake'
    try:
        client.head_bucket(Bucket=bucket)
        print(f'⚠️  Bucket {bucket} כבר קיים')
    except ClientError:
        client.create_bucket(Bucket=bucket)
        print(f'✅ Bucket נוצר: {bucket}')
    # יצירת prefix folders
    for prefix in ['raw/bus-positions/', 'raw/train-positions/',
                   'raw/trip-updates/', 'raw/service-alerts/',
                   'processed/delay-events/']:
        client.put_object(Bucket=bucket, Key=prefix, Body=b'')
    print('✅ כל ה-prefixes נוצרו')
except Exception as e:
    print(f'⚠️  MinIO: {e}')
EOF

# ─────────────────────────────────────────────────────────────
#  שלב 7: אתחול Redshift Schema (או PostgreSQL מקומי)
# ─────────────────────────────────────────────────────────────
step "7 — אתחול Data Warehouse Schema"

source .env 2>/dev/null || true

if [ -n "$REDSHIFT_HOST" ] && [ "$REDSHIFT_HOST" != "your-cluster.region.redshift.amazonaws.com" ]; then
  log "מאתחל Redshift schema..."
  python3 - <<'EOF'
import sys; sys.path.insert(0, '.')
try:
    from warehouse.redshift_writer import RedshiftWriter
    rw = RedshiftWriter()
    rw.create_schema()
    rw.close()
    print('✅ Redshift schema נוצר בהצלחה')
except Exception as e:
    print(f'⚠️  Redshift: {e}')
EOF
else
  warn "REDSHIFT_HOST לא מוגדר — מדלג (ניתן להגדיר מאוחר יותר ב-.env)"
fi

# ─────────────────────────────────────────────────────────────
#  שלב 8: הפעלת DAGs ב-Airflow
# ─────────────────────────────────────────────────────────────
step "8 — הפעלת Airflow DAGs"

sleep 5  # ממתין לסנכרון DAG files

for dag in "dag_realtime_ingestion" "dag_etl_transform" "dag_daily_analytics"; do
  docker compose exec -T airflow-webserver \
    airflow dags unpause "$dag" 2>/dev/null && \
    success "DAG מופעל: $dag" || \
    warn "לא ניתן להפעיל $dag (ייתכן שעדיין לא נטען)"
done

# ─────────────────────────────────────────────────────────────
#  שלב 9: הרצת בדיקת GTFS-RT ראשונה
# ─────────────────────────────────────────────────────────────
step "9 — בדיקת חיבור ל-GTFS-RT (נתוני תחבורה אמיתיים)"

log "מנסה לשלוף נתונים מ-MOT GTFS-RT..."
python3 - <<'EOF'
import sys; sys.path.insert(0, '.')
import requests

feeds = {
    'VehiclePositions (מיקומי אוטובוסים)': 'https://gtfs.mot.gov.il/gtfsfiles/VehiclePositions.pb',
    'TripUpdates (איחורים)':               'https://gtfs.mot.gov.il/gtfsfiles/TripUpdates.pb',
    'ServiceAlerts (התראות)':             'https://gtfs.mot.gov.il/gtfsfiles/ServiceAlerts.pb',
}
for name, url in feeds.items():
    try:
        r = requests.head(url, timeout=5)
        if r.status_code == 200:
            print(f'✅ {name}: זמין')
        else:
            print(f'⚠️  {name}: HTTP {r.status_code}')
    except Exception as e:
        print(f'❌ {name}: {e}')

# Israel Railways
try:
    r = requests.get('https://israelrail.azurewebsites.net/stations/GetStationBoard',
                     params={'stationId':'2300','date':'2025-01-01','hour':10}, timeout=5)
    print(f'✅ Israel Railways API: זמין (HTTP {r.status_code})')
except Exception as e:
    print(f'⚠️  Israel Railways API: {e}')
EOF

# ─────────────────────────────────────────────────────────────
#  שלב 10: הרצת Producer ראשוני (אחד לדוגמה)
# ─────────────────────────────────────────────────────────────
step "10 — הרצת Producer ראשון לבדיקה"

log "מריץ Bus Positions Producer לדגימה ראשונה..."
python3 - <<'EOF'
import sys; sys.path.insert(0, '.')
import os; os.environ.setdefault('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
try:
    from producers.bus_positions_producer import BusPositionsProducer
    p = BusPositionsProducer()
    p.run_once()
    p.close()
    print(f'✅ Producer הצליח: {p.stats["sent"]} הודעות נשלחו ל-Kafka')
except Exception as e:
    print(f'⚠️  Producer (לא קריטי): {e}')
EOF

# ─────────────────────────────────────────────────────────────
#  סיום — סיכום וקישורים
# ─────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}${GREEN}"
echo "  ╔══════════════════════════════════════════════════════╗"
echo "  ║           🎉  המערכת פעילה בהצלחה!                  ║"
echo "  ╚══════════════════════════════════════════════════════╝"
echo -e "${NC}"

echo -e "${BOLD}  🔗  כתובות גישה:${NC}"
echo -e "  ${CYAN}Airflow UI${NC}     →  http://localhost:8081  (admin / admin)"
echo -e "  ${CYAN}Kafka UI${NC}       →  http://localhost:8080"
echo -e "  ${CYAN}MinIO Console${NC}  →  http://localhost:9001  (minioadmin / minioadmin123)"
echo -e "  ${CYAN}Kibana${NC}         →  http://localhost:5601"
echo ""
echo -e "${BOLD}  📋  פקודות שימושיות:${NC}"
echo -e "  ${YELLOW}עצירה:${NC}           docker compose down"
echo -e "  ${YELLOW}לוגים:${NC}           docker compose logs -f kafka"
echo -e "  ${YELLOW}מצב שירותים:${NC}     docker compose ps"
echo -e "  ${YELLOW}Producer ידני:${NC}   source venv/bin/activate && python3 producers/bus_positions_producer.py"
echo ""
echo -e "${BOLD}  🚌  הפרויקט מורכב מ:${NC}"
echo -e "  • ${GREEN}4 Producers${NC} שולפים נתונים מ-GTFS-RT ורכבת ישראל"
echo -e "  • ${GREEN}6 Kafka Topics${NC} לסטרימינג בזמן אמת"
echo -e "  • ${GREEN}3 Airflow DAGs${NC} לתזמון ETL"
echo -e "  • ${GREEN}MinIO S3${NC} לאחסון (Data Lake)"
echo -e "  • ${GREEN}Redshift${NC} למחסן נתונים (Data Warehouse)"
echo ""