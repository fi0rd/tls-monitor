#!/opt/venv/bin/python3
import datetime

import requests
import yaml
import argparse
import logging
import redis
import json

from classes import DNSRecord, Group
from pathlib import Path

log = logging.getLogger("targets_exporter")


def get_hosts_for_dc(data_center: str, config: dict) -> list[DNSRecord]:
    api_tool_dnsrecords_url: str = config["OneCloudUrl"].replace("<datacenter>", data_center)
    try:
        response = requests.get(
            url=api_tool_dnsrecords_url,
            headers={"Accept": "application/json"},
            verify=False,
            timeout=1,
        )
        response.raise_for_status()
    except requests.RequestException as e:
        log.error(f"error fetching data from {api_tool_dnsrecords_url}: {e}")
        return []

    log.debug(f"inventory response status: {response.status_code}")
    dns_records = [DNSRecord(**item) for item in response.json()]
    return dns_records


def main():
    parser = argparse.ArgumentParser(description="targets_exporter")
    parser.add_argument(
        "--config", default="../../configs/targets_exporter.yml", help="path to config"
    )
    args = parser.parse_args()

    # Load configuration
    with open(args.config, "r") as f:
        config = yaml.safe_load(f)

    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.DEBUG,
        filename=config.get("LogFile") or "../targets_exporter.log",
    )
    logging.captureWarnings(True)

    redis_client = redis.StrictRedis(host=config.get("RedisHost") or "localhost",
                                     port=config.get("RedisPort") or 6379,
                                     username=config.get("RedisUsername") or "",
                                     password=config.get("RedisPassword") or "",
                                     db=0)

    prom_groups = []
    existing_hosts = set()

    data_centres = config.get("DataCenter")
    log.info(f"Total DC: {data_centres}")

    for data_center in config.get("DataCenter", []):
        hosts_dc = get_hosts_for_dc(data_center, config)
        log.info(f"Size of DNS Records for datacenter {data_center}: {len(hosts_dc)}")

        for host in hosts_dc:
            if host.host in existing_hosts:
                continue

            target = f"https://{host.host}"

            group = Group(
                targets=[target],
                labels={
                    "datacenter": data_center,
                    "namespace": host.namespace,
                    "ip": host.ip,
                    "hostname": host.host,
                },
            )

            redis_client.set(name=target,
                             value=json.dumps(group.__dict__),
                             ex=config.get("RedisTTL") or 3600)
            prom_groups.append(group)
            existing_hosts.add(host.host)

    log.info(f"{len(prom_groups)} targets saved in Redis")


if __name__ == "__main__":
    print(f"starting {__name__} at {datetime.datetime.now()}...")
    main()
