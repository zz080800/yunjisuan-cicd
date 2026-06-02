from flask import Flask, jsonify
import redis
import os
import pandas as pd   # 自选包示例，你也可以换成 requests

app = Flask(__name__)

redis_host = os.getenv("REDIS_HOST", "localhost")
redis_port = os.getenv("REDIS_PORT", 6379)
redis_password = os.getenv("REDIS_PASSWORD", "")

r = redis.Redis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)

@app.route('/api/ping')
def ping():
    # 顺便测试 Redis 连接
    try:
        r.ping()
        redis_status = "connected"
    except:
        redis_status = "failed"
    return jsonify({"status": "ok", "redis": redis_status})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)