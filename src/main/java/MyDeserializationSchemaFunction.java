
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * 自定义反序列化器，将FlinkCDC读取到的数据转为json格式
 */
public class MyDeserializationSchemaFunction implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        Struct valueStruct = (Struct) sourceRecord.value();
        Struct sourceStrut = valueStruct.getStruct("source");

        String database = sourceStrut.getString("db"); //获取数据库的名称
        String table = sourceStrut.getString("table"); //获取表名

        //获取类型（c-->insert,u-->update）
        String type = Envelope.operationFor(sourceRecord).toString().toLowerCase();
        if(type.equals("create")){
            type="insert";
        }
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("database",database);
        jsonObj.put("table",table);
        jsonObj.put("type",type);

        //获取数据data
        Struct afterStruct = valueStruct.getStruct("after");
        JSONObject dataJsonObj = new JSONObject();
        if(afterStruct!=null){
            for (Field field : afterStruct.schema().fields()) {
                String fieldName = field.name();
                Object fieldValue = afterStruct.get(field);
                dataJsonObj.put(fieldName,fieldValue);
            }
        }
        jsonObj.put("data",dataJsonObj);

        //向下游传递数据
        collector.collect(jsonObj.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}