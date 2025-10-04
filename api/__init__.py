import os
from datetime import timedelta

from quart import Quart, jsonify
from quart_jwt_extended import JWTManager
from quart_rate_limiter import RateLimiter

from api.core import metrics
from api.routes.health import health_bp
from api.routes.players import players_bp
from api.routes.groups import groups_bp
from api.routes.utils import utils_bp
from api.routes.webhook import webhook_bp
from api.worker import create_blueprint as create_worker_blueprint


def create_app() -> Quart:
    app = Quart(__name__)

    # Configure logging to suppress HTTP access logs
    import logging
    logging.getLogger('quart.serving').setLevel(logging.ERROR)
    logging.getLogger('hypercorn.access').setLevel(logging.CRITICAL + 1)
    logging.getLogger('hypercorn.access').disabled = True

    # Core config
    app.config["JWT_SECRET_KEY"] = os.getenv("JWT_TOKEN_KEY")
    app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(days=1)

    # Extensions
    JWTManager(app)
    RateLimiter(app)

    # Error handlers
    @app.errorhandler(404)
    async def _not_found(e):
        return jsonify({"error": "Resource not found"}), 404

    @app.errorhandler(500)
    async def _server_error(e):
        return jsonify({"error": "Internal server error"}), 500

    # Blueprints
    app.register_blueprint(create_worker_blueprint(), url_prefix='/')
    app.register_blueprint(health_bp, url_prefix='/')
    app.register_blueprint(players_bp, url_prefix='/')
    app.register_blueprint(groups_bp, url_prefix='/')
    app.register_blueprint(utils_bp, url_prefix='/')
    app.register_blueprint(webhook_bp, url_prefix='/')

    return app


__all__ = ["create_app"]


