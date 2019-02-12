import hazelcast
import logging

if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    config = hazelcast.ClientConfig()
    flake_id_generator_config = hazelcast.FlakeIdGeneratorConfig()

    # Default value is 600000 (10 minutes)
    flake_id_generator_config.prefetch_validity_in_millis = 30000

    # Default value is 100
    flake_id_generator_config.prefetch_count = 50

    config.add_flake_id_generator_config(flake_id_generator_config)
    client = hazelcast.HazelcastClient(config)

    generator = client.get_flake_id_generator("id-generator").blocking()

    for _ in range(100):
        print("Id: {}".format(generator.new_id()))

    client.shutdown()
