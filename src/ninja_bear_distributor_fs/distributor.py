import os

from typing import Dict
from os.path import join
from ninja_bear import DistributorBase, DistributeInfo
from ninja_bear.base.distributor_credentials import DistributorCredentials


class NoPathsException(Exception):
    def __init__(self) -> None:
        super().__init__('No paths provided')


class DestinationDoesntExistException(Exception):
    def __init__(self, destination: str) -> None:
        super().__init__(
            f'The destination directory \'{destination}\' ' \
            'doesn\'t exist. If it shall be created automatically, set create_parents to true'
        )


class Distributor(DistributorBase):
    """
    FileSystem specific distributor. For more information about the distributor methods,
    refer to DistributorBase.
    """
    def __init__(self, config: Dict, credentials: DistributorCredentials=None):
        super().__init__(config, credentials)

        paths, paths_exists = self.from_config('paths')

        if not paths_exists:
            raise NoPathsException()

        # Make sure _paths is a list.
        if not isinstance(paths, list):
            if not paths:
                paths = '.'
            paths = [paths]

        # Make sure _paths are directories.
        self._paths = paths
        self._create_parents, _ = self.from_config('create_parents')

    def _distribute(self, info: DistributeInfo) -> DistributorBase:
        """
        Distributes the generated config. Here goes all the logic to distribute the generated
        config according to the plugin's functionality (e.g. commit to Git, copy to a different
        directory, ...).

        :param info: Contains the required information to distribute the generated config.
        :type info:  DistributeInfo
        """
        parent = info.input_path.parent
        parent = str(parent.absolute()) if parent else ''

        for path in self._paths:
            destination_path = join(parent, path)

            # Create parents if required.
            if destination_path and self._create_parents:
                os.makedirs(destination_path, exist_ok=True)

            # Make sure target directory exists.
            if not os.path.exists(destination_path):
                raise DestinationDoesntExistException(destination_path)
            
            # Write files to destination path.
            with open(join(destination_path, info.file_name), 'w') as f:
                f.write(info.data)
