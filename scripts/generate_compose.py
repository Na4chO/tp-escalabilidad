"""Generates docker-compose.yaml with the given amount of instances of each scalable node.

  python3 scripts/generate_compose.py [--metadata-filter N] [--abstract-preprocessing N]
      [--frequency-collector N] [--top-aggregator N] [--average-calculator N] [--word-counter N]
      [--clients N] [--batch-size N] [--output FILE]

The server, rabbitmq and the 2 image fetchers are fixed.
"""

import argparse

IMAGE_FETCHERS = 2
# Bandwidth limit of each image fetcher for the Wikimedia downloads
DOWNLOAD_RATE_MBPS = 25
TOP_SIZE = 20
MOM = ["MOM_HOST=rabbitmq", "MOM_PORT=5672"]
USER_AGENT = "SistemasDistribuidos-Grupo29/1.0 (https://github.com/Na4chO/tp-escalabilidad)"


def service(name, node, environment, depends_on_rabbit=True, extra=""):
    lines = [
        f"  {name}:",
        "    build:",
        "      context: ./src",
        f"      dockerfile: {node}/Dockerfile",
        f"    container_name: {name}",
    ]
    if depends_on_rabbit:
        lines += ["    depends_on:", "      rabbitmq:", "        condition: service_healthy"]
    lines += ["    environment:"] + [f"      - {variable}" for variable in environment]
    return "\n".join(lines) + extra + "\n"


def replicas(node, amount, environment):
    """environment(i) returns the variables of instance i."""
    return [service(f"{node}_{i}", node, environment(i)) for i in range(amount)]


def clients(amount, batch_size):
    """Each client sends the files of data/client_<i> (see scripts/split_data.sh) and writes to output/client_<i>."""
    return [service(f"client_{i}", "client",
                    ["SERVER_HOST=server", "SERVER_PORT=5678", "INPUT_PATH=/data", "OUTPUT_PATH=/output",
                     f"BATCH_SIZE={batch_size}"],
                    depends_on_rabbit=False,
                    extra="\n    depends_on:\n      - server"
                          f"\n    volumes:\n      - ./data/client_{i}:/data:ro\n      - ./output/client_{i}:/output")
            for i in range(amount)]


def generate(args):
    # Every node gets the broker and the max items per message
    node = [*MOM, f"BATCH_SIZE={args.batch_size}"]
    services = [
        service("server", "server",
                ["SERVER_HOST=0.0.0.0", "SERVER_PORT=5678", *node, "SERVER_ID=1"],
                extra='\n    ports:\n      - "5678:5678"'),
        *replicas("metadata_filter", args.metadata_filter,
                  lambda i: [*node, f"INSTANCE_ID={i}", f"IMAGE_FETCHER_COUNT={IMAGE_FETCHERS}"]),
        *replicas("image_fetcher", IMAGE_FETCHERS,
                  lambda i: [*node, f"WIKIMEDIA_USER_AGENT={USER_AGENT}", "IMAGE_CACHE_SIZE=100", f"PARTITION={i}",
                             f"DOWNLOAD_RATE_MBPS={DOWNLOAD_RATE_MBPS}"]),
        *replicas("abstract_preprocessing", args.abstract_preprocessing,
                  lambda i: [*node, f"INSTANCE_ID={i}", f"FREQUENCY_COLLECTOR_AMOUNT={args.frequency_collector}",
                             f"AVERAGE_CALCULATOR_AMOUNT={args.average_calculator}"]),
        *replicas("frequency_collector", args.frequency_collector,
                  lambda i: [*node, f"TOP_SIZE={TOP_SIZE}", f"ID={i}", f"TOP_AGGREGATOR_AMOUNT={args.top_aggregator}"]),
        *replicas("top_aggregator", args.top_aggregator,
                  lambda i: [*node, f"TOP_SIZE={TOP_SIZE}", f"ID={i}"]),
        *replicas("average_calculator", args.average_calculator,
                  lambda i: [*node, f"ID={i}"]),
        *replicas("abstract_word_counter", args.word_counter, lambda i: [*node, f"INSTANCE_ID={i}"]),
        *clients(args.clients, args.batch_size),
        RABBITMQ,
    ]
    return "services:\n" + "\n".join(services)


RABBITMQ = """  rabbitmq:
    build:
      context: ./src/rabbitmq
      dockerfile: Dockerfile
    container_name: rabbitmq
    environment:
      - RABBITMQ_LOG_LEVELS=error
    healthcheck:
      interval: 5s
      retries: 10
      start_period: 50s
      test: rabbitmq-diagnostics check_port_connectivity
      timeout: 3s
    ports:
      - 5672:5672
      - 15672:15672
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates docker-compose.yaml")
    for node in ["clients", "metadata-filter", "abstract-preprocessing", "frequency-collector", "top-aggregator",
                 "average-calculator", "word-counter"]:
        parser.add_argument(f"--{node}", type=int, default=1, help="instances (default 1)")
    parser.add_argument("--batch-size", type=int, default=1, help="max items per message (default 1)")
    parser.add_argument("--output", default="docker-compose.yaml")
    args = parser.parse_args()

    compose = generate(args)  # before opening the file, so an error doesn't leave it empty
    with open(args.output, "w") as file:
        file.write(compose)
    print(f"{args.output}: batch_size={args.batch_size} clients={args.clients} metadata_filter={args.metadata_filter} abstract_preprocessing={args.abstract_preprocessing} "
          f"frequency_collector={args.frequency_collector} top_aggregator={args.top_aggregator} average_calculator={args.average_calculator} "
          f"abstract_word_counter={args.word_counter} image_fetcher={IMAGE_FETCHERS}")
