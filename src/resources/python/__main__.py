from py4j.java_gateway import JavaGateway
from python_service_factory import PythonServiceFactory

if __name__ == '__main__':
    print('!!!Python __main__')

    print("!!!Python A")
    gateway = JavaGateway(start_callback_server=True)
    print("!!!Python B")
    gateway.entry_point.pythonEntry(PythonServiceFactory())
    print("!!!Python C")
    # gateway.shutdown()
