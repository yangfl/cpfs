import os


class StorageOperations:
    '''Local storage with terrible performance'''
    def __init__(self, hostname, path, username, password,
                 **additional_options):
        if not os.path.isdir(path):
            raise ValueError("'{}' not a directory\n".format(path))
        self.path = path

    def close(self, name):
        pass

    def create(self, name):
        os.mknod(os.path.join(self.path, name))

    def destory(self):
        pass

    def flush(self, name):
        pass

    def read(self, name, offset, length):
        file_path = os.path.join(self.path, name)
        with open(file_path, 'rb') as file_handle:
            file_handle.seek(offset)
            return file_handle.read(length)

    def open(self, name):
        pass

    def remove(self, name):
        os.remove(os.path.join(self.path, name))

    def size(self, name):
        return os.stat(os.path.join(self.path, name)).st_size

    def statfs(self):
        return (100, 10000)

    def truncate(self, name, length):
        with open(os.path.join(self.path, name), 'ab') as file_handle:
            file_handle.truncate(length)

    def write(self, name, offset, buf):
        with open(os.path.join(self.path, name), 'rb+') as file_handle:
            file_handle.seek(offset)
            file_handle.write(buf)
        return len(buf)
