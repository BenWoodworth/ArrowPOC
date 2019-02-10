class PythonServiceFactory:
    class Java:
        implements = ['ServiceFactory']

    def createReadWriteFile(self, file: str) -> ReadWriteFile:
        # TODO
        pass

    def createReadWritePlasma(self, storePath: str) -> ReadWritePlasma:
        # TODO
        pass

    def createSerializeJson(self) -> SerializeJson:
        # TODO
        pass

    def createSerializeParquet(self) -> SerializeParquet:
        # TODO
        pass
