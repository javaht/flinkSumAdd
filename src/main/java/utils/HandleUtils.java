package utils;

import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import windowSum.MyDeserializationSchemaFunction;
public class HandleUtils {

    public static SourceFunction<String> postgreSourceFunction(String ip,
                                                               Integer port,
                                                               String database,
                                                               String schemaList,
                                                               String tableList,
                                                               String slotName,
                                                               String username,
                                                               String password
    ){
        SourceFunction<String> sourceFunction = PostgreSQLSource.<String>builder()
                .hostname(ip)
                .port(port)
                .database(database)
                .schemaList(schemaList)
                .tableList(tableList)
                .slotName(slotName)
                .decodingPluginName("pgoutput")
                .username(username)
                .password(password)
                .deserializer(new MyDeserializationSchemaFunction())
                .build();

        return sourceFunction;
    }




}
