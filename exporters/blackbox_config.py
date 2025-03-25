#!/opt/venv/bin/python3

import redis
import yaml
import json
import argparse
import logging

from classes import Group


log = logging.getLogger("blackbox_targets_config")


def main():
    parser = argparse.ArgumentParser(description="Create file with blackbox exporter targets")
    parser.add_argument(
        "--config", default="../../configs/blackbox_targets_config.yml", help="path to config"
    )
    args = parser.parse_args()

    # Load configuration
    with open(args.config, "r") as f:
        config = yaml.safe_load(f)

    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.DEBUG,
        filename=config.get("LogFile") or "../blackbox_targets_config.log",
    )
    logging.captureWarnings(True)

    redis_client = redis.StrictRedis(host=config.get("RedisHost") or "localhost",
                                     port=config.get("RedisPort") or 6379,
                                     username=config.get("RedisUsername") or "",
                                     password=config.get("RedisPassword") or "",
                                     db=0)

    keys: list = redis_client.keys()

    if not keys:
        log.error("No keys found in Redis")
        return

    targets_list = []

    for key in keys:
        value: bytes = redis_client.get(key)
        if value:
            try:
                service: Group = Group(**json.loads(value.decode('utf-8')))
                service.labels["check_ssl_by_ip"] = str(service.check_ssl_by_ip).lower()

                if service.alive:
                    targets_list.append({
                        'targets': service.targets,
                        'labels': service.labels or {},
                    })
            except json.JSONDecodeError:
                log.error(f"json decode error: {key.decode('utf-8')}")

    if not targets_list:
        log.error("no targets found")
        return

    with open(config.get("OutputFile"), 'w') as f:
        log.info("Creating target file...")
        for target in targets_list:
            yaml.dump([target], f, default_flow_style=False, sort_keys=False)

    log.info(f"Blackbox Exporter {len(targets_list)} targets added to file: {config.get('OutputFile')}")


if __name__ == "__main__":
    print(f"starting {__name__} ...")
    main()
