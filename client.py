from abstractClient import AbstractClient


class Client(AbstractClient):

    def __init__(self, cluster=None):
        if cluster is None:
            cluster = [("127.0.0.1", 5254)]
        self.data = {'cluster': cluster}
        self.data = self._get_state()
        self.append_retry_attempts = 3

    def create(self, key, value):
        self._append_log({'action': 'change', 'key': key, 'value': value})

    def get_data(self, key):
        self.data = self._get_state()
        if key in self.data:
            return self.data[key]
        return "NOT FOUND"

    def set_data(self, key, value):
        self._append_log({'action': 'change', 'key': key, 'value': value})

    def delete(self, key):
        self._append_log({'action': 'delete', 'key': key})

    def _append_log(self, payload):
        response = None
        for attempt in range(self.append_retry_attempts):
            response = super()._append_log(payload)
            print(response)
            if response['success']:
                return
        # TODO: logging
        return response


if __name__ == '__main__':
    c = Client()
    c.create("alpha", "beta")
    print(c.get_data("alpha"))
    c.delete("alpha")
    print(c.get_data("alpha"))
    print(c.data)