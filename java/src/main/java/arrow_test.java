import static java.util.Arrays.asList;
import java.io.IOException;
import org.apache.arrow.plasma.*;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Interval;
import org.apache.arrow.vector.types.pojo.ArrowType.List;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;


public class arrow_test {

    private static Field field(String name, boolean nullable, ArrowType type, Field... children) {
        return new Field(name, new FieldType(nullable, type, null, null), asList(children));
    }

    private static Field field(String name, ArrowType type, Field... children) {
        return field(name, true, type, children);
    }

    
    public void testComplex() throws IOException {
        Schema schema = new Schema(asList(
                field("a", false, new Int(8, true)),
                field("b", new Struct(),
                        field("c", new Int(16, true)),
                        field("d", new Utf8())),
                field("e", new List(), field(null, new Date(DateUnit.MILLISECOND))),
                field("f", new FloatingPoint(FloatingPointPrecision.SINGLE)),
                field("g", new Timestamp(TimeUnit.MILLISECOND, "UTC")),
                field("h", new Timestamp(TimeUnit.MICROSECOND, null)),
                field("i", new Interval(IntervalUnit.DAY_TIME))
        ));
    }
    
    public void testAll() throws IOException {
        Schema schema = new Schema(asList(
                field("a", false, new Null()),
                field("b", new Struct(), field("ba", new Null())),
                field("c", new List(), field("ca", new Null())),
                field("d", new Union(UnionMode.Sparse, new int[] {1, 2, 3}), field("da", new Null())),
                field("e", new Int(8, true)),
                field("f", new FloatingPoint(FloatingPointPrecision.SINGLE)),
                field("g", new Utf8()),
                field("h", new Binary()),
                field("i", new Bool()),
                field("j", new Decimal(5, 5)),
                field("k", new Date(DateUnit.DAY)),
                field("l", new Date(DateUnit.MILLISECOND)),
                field("m", new Time(TimeUnit.SECOND, 32)),
                field("n", new Time(TimeUnit.MILLISECOND, 32)),
                field("o", new Time(TimeUnit.MICROSECOND, 64)),
                field("p", new Time(TimeUnit.NANOSECOND, 64)),
                field("q", new Timestamp(TimeUnit.MILLISECOND, "UTC")),
                field("r", new Timestamp(TimeUnit.MICROSECOND, null)),
                field("s", new Interval(IntervalUnit.DAY_TIME)),
                field("t", new FixedSizeBinary(100))
        ));

        System.out.println(schema.toString());
        PlasmaClient client = new PlasmaClient("tmp/plasma", "", 0);
        System.loadLibrary("plasma_java");
        byte[] i_d = {1};
        client.put(i_d, schema.toByteArray(), null);

    }

    
    public void testUnion() throws IOException {
        Schema schema = new Schema(asList(
                field("d", new Union(UnionMode.Sparse, new int[] {1, 2, 3}), field("da", new Null()))
        ));
    }

    
    public void testDate() throws IOException {
        Schema schema = new Schema(asList(
                field("a", new Date(DateUnit.DAY)),
                field("b", new Date(DateUnit.MILLISECOND))
        ));
    }

    
    public void testTime() throws IOException {
        Schema schema = new Schema(asList(
                field("a", new Time(TimeUnit.SECOND, 32)),
                field("b", new Time(TimeUnit.MILLISECOND, 32)),
                field("c", new Time(TimeUnit.MICROSECOND, 64)),
                field("d", new Time(TimeUnit.NANOSECOND, 64))
        ));
    }

    
    public void testTS() throws IOException {
        Schema schema = new Schema(asList(
                field("a", new Timestamp(TimeUnit.SECOND, "UTC")),
                field("b", new Timestamp(TimeUnit.MILLISECOND, "UTC")),
                field("c", new Timestamp(TimeUnit.MICROSECOND, "UTC")),
                field("d", new Timestamp(TimeUnit.NANOSECOND, "UTC")),
                field("e", new Timestamp(TimeUnit.SECOND, null)),
                field("f", new Timestamp(TimeUnit.MILLISECOND, null)),
                field("g", new Timestamp(TimeUnit.MICROSECOND, null)),
                field("h", new Timestamp(TimeUnit.NANOSECOND, null))
        ));
    }

    
    public void testInterval() throws IOException {
        Schema schema = new Schema(asList(
                field("a", new Interval(IntervalUnit.YEAR_MONTH)),
                field("b", new Interval(IntervalUnit.DAY_TIME))
        ));
    }

    
    public void testFP() throws IOException {
        Schema schema = new Schema(asList(
                field("a", new FloatingPoint(FloatingPointPrecision.HALF)),
                field("b", new FloatingPoint(FloatingPointPrecision.SINGLE)),
                field("c", new FloatingPoint(FloatingPointPrecision.DOUBLE))
        ));
    }


    public static void main(String[] args) {
        System.loadLibrary("plasma_java");
        arrow_test t = new arrow_test();
        try{
            t.testAll();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}