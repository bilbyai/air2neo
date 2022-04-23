# run fastapi server
# Local .env
if [ -f .env ]; then
    # Load Environment Variables
    export $(cat .env | grep -v '#' | sed 's/\r$//' | awk '/=/ {print $1}' )
fi
uvicorn app.main:app --reload --host 0.0.0.0 --port 8080