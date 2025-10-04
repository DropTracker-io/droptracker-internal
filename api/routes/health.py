from datetime import datetime

from quart import Blueprint, jsonify

from api.health_utils import health_check_lightweight


health_bp = Blueprint("health", __name__)


@health_bp.get("/ping")
async def ping():
    return jsonify({"message": "Pong"}), 200


@health_bp.get("/health")
async def health_endpoint():
    try:
        health_status = await health_check_lightweight()
        if health_status["healthy"]:
            return jsonify({
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "server": "api.droptracker.io",
                "checks": health_status["checks"],
            }), 200
        else:
            return jsonify({
                "status": "unhealthy",
                "timestamp": datetime.now().isoformat(),
                "server": "api.droptracker.io",
                "checks": health_status["checks"],
            }), 503
    except Exception as e:
        return jsonify({
            "status": "error",
            "timestamp": datetime.now().isoformat(),
            "server": "api.droptracker.io",
            "error": str(e),
        }), 500


