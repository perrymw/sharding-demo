import json
import os
from shutil import copyfile
from typing import List, Dict

filename = "chapter2.txt"


def load_data_from_file(path=None) -> str:
    with open(path if path else filename, 'r') as f:
        data = f.read()
    return data


class ShardHandler(object):
    """
    Take any text file and shard it into X number of files with
    Y number of replications.
    """

    def __init__(self):
        self.mapping = self.load_map()
        self.last_char_position = 0

    mapfile = "mapping.json"

    def write_map(self) -> None:
        """Write the current 'database' mapping to file."""
        with open(self.mapfile, 'w') as m:
            json.dump(self.mapping, m, indent=2)

    def load_map(self) -> Dict:
        """Load the 'database' mapping from file."""
        if not os.path.exists(self.mapfile):
            return dict()
        with open(self.mapfile, 'r') as m:
            return json.load(m)

    def _reset_char_position(self):
        self.last_char_position = 0

    def get_shard_ids(self):
        return sorted([key for key in self.mapping.keys() if '-' not in key])

    def get_replication_ids(self):
        return sorted([key for key in self.mapping.keys() if '-' in key])

    def build_shards(self, count: int, data: str = None) -> [str, None]:
        """Initialize our miniature databases from a clean mapfile. Cannot
        be called if there is an existing mapping -- must use add_shard() or
        remove_shard()."""
        if self.mapping != {}:
            return "Cannot build shard setup -- sharding already exists."

        spliced_data = self._generate_sharded_data(count, data)

        for num, d in enumerate(spliced_data):
            self._write_shard(num, d)

        self.write_map()

    def _write_shard_mapping(self, num: str, data: str, replication=False):
        """Write the requested data to the mapfile. The optional `replication`
        flag allows overriding the start and end information with the shard
        being replicated."""
        if replication:
            parent_shard = self.mapping.get(num[:num.index('-')])
            self.mapping.update(
                {
                    num: {
                        'start': parent_shard['start'],
                        'end': parent_shard['end']
                    }
                }
            )
        else:
            if int(num) == 0:
                # We reset it here in case we perform multiple write operations
                # within the same instantiation of the class. The char position
                # is used to power the index creation.
                self._reset_char_position()

            self.mapping.update(
                {
                    str(num): {
                        'start': (
                            self.last_char_position if
                            self.last_char_position == 0 else
                            self.last_char_position + 1
                        ),
                        'end': self.last_char_position + len(data)
                    }
                }
            )

            self.last_char_position += len(data)

    def _write_shard(self, num: int, data: str) -> None:
        """Write an individual database shard to disk and add it to the
        mapping."""
        if not os.path.exists("data"):
            os.mkdir("data")
        with open(f"data/{num}.txt", 'w') as s:
            s.write(data)
        self._write_shard_mapping(str(num), data)


# help received from Scott Reese
    def _remove_file(self, key) -> None:
        """Removing an individual database shard to disk and 
        remove from the mapping."""
        files = os.listdir('./data/')
        original = sorted([f for f in files if '-' not in f],
                            key=lambda x: int(x[:-4]))
        replicates = sorted([f for f in files if '-' in f],
                            key=lambda x: int(x.split('-')[0]))
        last_original = original[-1]

        for f in replicates:
            target_number = f.split('-')[0] + '.txt'
            if last_original == target_number:
                os.remove(f'./data/{f}')

        os.remove(f'./data/{last_original}')
        self.mapping.pop(key, None)

    def _generate_sharded_data(self, count: int, data: str) -> List[str]:
        """Split the data into as many pieces as needed."""
        splicenum, rem = divmod(len(data), count)

        result = [data[splicenum * z:splicenum * (z + 1)] for z in range(count)]
        # take care of any odd characters
        if rem > 0:
            result[-1] += data[-rem:]

        return result

    def load_data_from_shards(self) -> str:
        """Grab all the shards, pull all the data, and then concatenate it."""
        result = list()

        for db in self.get_shard_ids():
            with open(f'data/{db}.txt', 'r') as f:
                result.append(f.read())
        return ''.join(result)

    def get_replication_level(self) -> None:
        keys = [k[-1] for k in self.get_replication_ids()]
        if not keys:
            return 0
        return(int(max(keys)))

    def add_shard(self) -> None:
        """Add a new shard to the existing pool and rebalance the data."""
        self.mapping = self.load_map()
        data = self.load_data_from_shards()
        keys = [int(z) for z in self.get_shard_ids()]
        keys.sort()
        # why 2? Because we have to compensate for zero indexing
        new_shard_num = max(keys) + 2

        spliced_data = self._generate_sharded_data(new_shard_num, data)

        for num, d in enumerate(spliced_data):
            self._write_shard(num, d)

        self.write_map()

        self.sync_replication()

    def remove_shard(self) -> None:
        """Loads the data from all shards, removes the extra 'database' file,
        and writes the new number of shards to disk.
        """
        self.mapping = self.load_map()
        data = self.load_data_from_shards()
        keys = [int(z) for z in self.get_shard_ids()]
        keys.sort()
        new_shard_num = max(keys)

        spliced_data = self._generate_sharded_data(new_shard_num, data)
        files = list(self.mapping.keys())
        target = files[-1]
        self._remove_file(target)
        for num, d in enumerate(spliced_data):
            self._write_shard(num, d)
        self.write_map()

        self.sync_replication()
# Alec's help
    def add_replication(self) -> None:
        """Add a level of replication so that each shard has a backup. Label
        them with the following format:

        1.txt (shard 1, primary)
        1-1.txt (shard 1, replication 1)
        1-2.txt (shard 1, replication 2)
        2.txt (shard 2, primary)
        2-1.txt (shard 2, replication 1)
        ...etc.

        By default, there is no replication -- add_replication should be able
        to detect how many levels there are and appropriately add the next
        level.
        """
        self.replication_level = self.get_replication_level()

        self.replication_level += 1

        files_dir = os.listdir('./data')

        list_of_data = []
        for f in files_dir:
            if '-' not in f:
                list_of_data.append(f)

        for number, f in enumerate(sorted(files_dir)):
            base_file = f'./data/{f}'
            replication_file = f'./data/{number}-{self.replication_level}.txt'
            copyfile(base_file, replication_file)
        
        for number, f in enumerate(self.get_shard_ids()):
            self.mapping[f'{number}-{self.replication_level}'] = self.mapping[f]

        self.write_map()

        self.sync_replication()

# Alec's guidance
    def remove_replication(self) -> None:
        """Remove the highest replication level.

        If there are only primary files left, remove_replication should raise
        an exception stating that there is nothing left to remove.

        For example:

        1.txt (shard 1, primary)
        1-1.txt (shard 1, replication 1)
        1-2.txt (shard 1, replication 2)
        2.txt (shard 2, primary)
        etc...

        to:

        1.txt (shard 1, primary)
        1-1.txt (shard 1, replication 1)
        2.txt (shard 2, primary)
        etc...
        """
        self.replication_level = self.get_replication_level()

        primary_keys= self.get_shard_ids()

        if self.replication_level:
            files_dir = os.listdir('./data')
            for f in files_dir:
                if '-' in f and int(f[-5]) == self.replication_level:
                    os.remove(f'./data/{f}')
            for number, f in enumerate(primary_keys):
                highest_replication = f'{number}-{self.replication_level}'
                self.mapping.pop(highest_replication)
            self.write_map()
            self.replication_level -= 1
        print('There is no replication.')
# Alec's help
    def sync_replication(self) -> None:
        """Verify that all replications are equal to their primaries and that
        any missing primaries are appropriately recreated from their
        replications."""
        # check source files
        def source_file_check():
            source_files = []
            replication_files = []
            recreate_replicates = []
            files_dir = os.listdir('./data')
            for f in files_dir:
                if '-' in f:
                    replication_files.append(f)
                source_files.append(f)
            for replicate in replication_files:
                source_number = replicate[0]
                replicate_stat = os.stat(f'./data/{replicate}')
                replicate_size = replicate_stat.st_size
                source_stat = os.stat(f'./data/{source_number}.txt')
                source_size = source_stat.st_size
                if source_size is not replicate_size:
                    recreate_replicates.append(replicate)
            return recreate_replicates
        # recreate replications
        def replication_file_recreated(files):
            for f in files:
                source_number = f[0]
                copyfile(f'./data/{source_number}.txt', f'./data/{f}')
        # recreate source files
        def source_file_recreated():
            self.replication_level = self.get_replication_level()
            keys = self.get_shard_ids()
            for key in keys:
                if not os.path.exists(f'./data/{key}.txt'):
                    copyfile(f'./data/{key}-{self.replication_leve}.txt', f'./data/{key}.txt')
    
        source_file_recreated()
        replication_file_recreated(source_file_check())





    def get_shard_data(self, shardnum=None) -> [str, Dict]:
        """Return information about a shard from the mapfile."""
        if not shardnum:
            return self.get_all_shard_data()
        data = self.mapping.get(shardnum)
        if not data:
            return f"Invalid shard ID. Valid shard IDs: {self.get_shard_ids()}"
        return f"Shard {shardnum}: {data}"

    def get_all_shard_data(self) -> Dict:
        """A helper function to view the mapping data."""
        return self.mapping


s = ShardHandler()

# s.build_shards(5, load_data_from_file())

# print(s.mapping.keys())


# print(s.mapping.keys())

# s.add_replication()
s.remove_replication()
print(s.mapping.keys())