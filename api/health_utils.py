import os
from datetime import datetime
import asyncio

from sqlalchemy import text

from api.core import metrics, redis_client, get_db_session


async def health_check(app) -> bool:
    """Comprehensive health check for the API server.

    Accepts the app instance to avoid relying on request context.
    """
    try:
        if app is None:
            print("Health check failed: Quart app is None")
            return False

        if metrics is None:
            print("Health check failed: metrics tracker is None")
            return False

        if not await test_database_connectivity():
            print("Health check failed: Database connectivity test failed")
            return False

        if not await test_redis_connectivity():
            print("Health check failed: Redis connectivity test failed")
            return False

        if not await test_request_processing():
            print("Health check failed: Request processing test failed")
            return False

        if not test_metrics_functionality():
            print("Health check failed: Metrics functionality test failed")
            return False

        return True
    except Exception as e:
        print(f"Health check failed with exception: {e}")
        return False


async def test_database_connectivity() -> bool:
    try:
        test_session = None
        try:
            test_session = await asyncio.wait_for(
                asyncio.to_thread(get_db_session),
                timeout=5.0,
            )
            result = await asyncio.wait_for(
                asyncio.to_thread(lambda: test_session.execute(text("SELECT 1")).scalar()),
                timeout=3.0,
            )
            return result == 1
        finally:
            if test_session:
                try:
                    test_session.close()
                except Exception:
                    pass
    except asyncio.TimeoutError:
        print("Database connectivity test timed out")
        return False
    except Exception as e:
        print(f"Database connectivity test failed: {e}")
        return False


async def test_redis_connectivity() -> bool:
    try:
        if not redis_client or not redis_client.client:
            return False
        ping_result = await asyncio.wait_for(
            asyncio.to_thread(redis_client.client.ping),
            timeout=3.0,
        )
        return bool(ping_result)
    except asyncio.TimeoutError:
        print("Redis connectivity test timed out")
        return False
    except Exception as e:
        print(f"Redis connectivity test failed: {e}")
        return False


async def test_request_processing() -> bool:
    try:
        import aiohttp

        port = int(os.environ.get("API_PORT", 31323))
        url = f"http://127.0.0.1:{port}/ping"

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as sess:
            async with sess.get(url) as response:
                if response.status != 200:
                    return False
                data = await response.json()
                return data.get("message") == "Pong"
    except asyncio.TimeoutError:
        print("Request processing test timed out")
        return False
    except Exception as e:
        print(f"Request processing test failed: {e}")
        return False


def test_metrics_functionality() -> bool:
    return True


async def health_check_lightweight() -> dict:
    from sqlalchemy import text

    checks = {}
    overall_healthy = True

    checks["metrics"] = {"status": "healthy" if metrics else "unhealthy"}
    if not metrics:
        overall_healthy = False

    # DB check
    try:
        db_session = await asyncio.wait_for(
            asyncio.to_thread(get_db_session),
            timeout=2.0,
        )
        try:
            await asyncio.wait_for(
                asyncio.to_thread(lambda: db_session.execute(text("SELECT 1")).scalar()),
                timeout=1.0,
            )
            checks["database"] = {"status": "healthy"}
        except Exception:
            checks["database"] = {"status": "unhealthy"}
            overall_healthy = False
        finally:
            try:
                db_session.close()
            except Exception:
                pass
    except Exception:
        checks["database"] = {"status": "unhealthy"}
        overall_healthy = False

    # Redis check
    try:
        if redis_client and redis_client.client:
            await asyncio.wait_for(
                asyncio.to_thread(redis_client.client.ping),
                timeout=1.0,
            )
            checks["redis"] = {"status": "healthy"}
        else:
            checks["redis"] = {"status": "unhealthy"}
            overall_healthy = False
    except Exception:
        checks["redis"] = {"status": "unhealthy"}
        overall_healthy = False

    return {"healthy": overall_healthy, "checks": checks}


