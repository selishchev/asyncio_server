import asyncio

metrics = {}


class ClientServerProtocol(asyncio.Protocol):

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        resp = self.process_data(data.decode('utf-8').strip('\n'))
        self.transport.write(resp.encode('utf-8'))

    def process_data(self, dat):
        pieces = dat.split(' ')
        if pieces[0] == 'get':
            return self.get(pieces[1])
        elif pieces[0] == 'put':
            return self.put(pieces[1], pieces[2], pieces[3])
        else:
            return 'error\nwrong command\n\n'

    @staticmethod
    def put(key, value, timestamp):
        if key == '*':
            return 'error\nwrong command\n\n'
        if key not in metrics:
            metrics[key] = []
        if (timestamp, value) not in metrics[key]:
            metrics[key].append((timestamp, value))
            metrics[key].sort(key=lambda x: x[0])
        return 'ok\n\n'

    @staticmethod
    def get(key):
        response = 'ok\n'
        if key == '*':
            for key, values in metrics.items():
                for value in values:
                    response += key + ' ' + value[1] + ' ' + value[0] + '\n'
        else:
            if key in metrics:
                for value in metrics[key]:
                    response += key + ' ' + value[1] + ' ' + value[0] + '\n'
        return response + '\n'


def run_server(host, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(ClientServerProtocol, host, port)
    server = loop.run_until_complete(coro)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


if __name__ == '__main__':
    run_server('127.0.0.1', 8888)
