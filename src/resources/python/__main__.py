from py4j.java_gateway import JavaGateway

if __name__ == '__main__':
    println('hello world')

    gateway = JavaGateway(start_callback_server=True)

    # "Sends" python object to the Java side.
    numbers = operator_example.randomBinaryOperator(operator)
