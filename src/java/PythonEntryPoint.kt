import test.ReadWrite
import test.Serialize

class PythonEntryPoint(
    private val callback: (
        List<PerformanceTester.ServiceInfo<ReadWrite>>,
        List<PerformanceTester.ServiceInfo<Serialize>>
    ) -> Unit
) {

    fun init(
        readWriteServices: List<PerformanceTester.ServiceInfo<ReadWrite>>,
        serializeServices: List<PerformanceTester.ServiceInfo<Serialize>>
    ) {
        callback(readWriteServices, serializeServices)
    }

    fun readWriteInfo(
        platform: String,
        format: String,
        service: ReadWrite
    ): PerformanceTester.ServiceInfo<ReadWrite> {
        return PerformanceTester.ServiceInfo(platform, format, service)
    }

    fun serializeInfo(
        platform: String,
        format: String,
        service: Serialize
    ): PerformanceTester.ServiceInfo<Serialize> {
        return PerformanceTester.ServiceInfo(platform, format, service)
    }
}