package com.paimonperf.generator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * OGG-JSON 信封格式验证：生成样本数据并核对结构是否符合 Flink {@code ogg-json} format 规范。
 *
 * <p>验证 op_type 与 before/after 的对应关系（驱动 ogg-json 推导 RowKind）：
 * <ul>
 *   <li>I：before=null, after=data</li>
 *   <li>U：before=data, after=data</li>
 *   <li>D：before=data, after=null</li>
 * </ul>
 */
class OggJsonFormatVerification {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void printSampleOggJsonRecords() throws Exception {
        RecordFactory factory = new RecordFactory(1000L, 0.4, 0.1);

        System.out.println("=== 生成10条样本 OGG 信封（INSERT/UPDATE/DELETE混合）===");
        for (int i = 0; i < 10; i++) {
            WideRecord r = factory.next();
            String json = r.toJson("lttsv7.KDPA_ZHXINX", "7", "clusterID1", 1042072462L + i);
            System.out.println(r.type + " | " + json);
        }

        System.out.println("\n=== 格式检查 ===");

        // INSERT：before=null, after=业务镜像
        WideRecord insert = factory.next();
        while (insert.type != RecordType.INSERT) {
            insert = factory.next();
        }
        JsonNode ins = MAPPER.readTree(insert.toJson("db.T", "1", "c1", 100L));
        assertEquals("I", ins.get("op_type").asText(), "INSERT op_type=I");
        assertTrue(ins.get("before").isNull(), "INSERT before 应为 null");
        assertFalse(ins.get("after").isNull(), "INSERT after 应为业务镜像");
        assertTrue(ins.get("after").has("pk"), "after 镜像应含 pk");
        assertTrue(ins.get("after").has("c1_bigint"), "after 镜像应含业务列");
        assertTrue(ins.get("after").has("event_time"), "after 镜像应含 event_time");
        // 信封元数据字段齐全
        for (String f : new String[]{"table", "op_type", "current_ts", "op_ts", "pos", "ddl", "groupId", "clusterName"}) {
            assertTrue(ins.has(f), "信封应含字段: " + f);
        }

        // UPDATE：before 与 after 均为业务镜像，且主键一致
        WideRecord update = factory.next();
        while (update.type != RecordType.UPDATE) {
            update = factory.next();
        }
        JsonNode upd = MAPPER.readTree(update.toJson("db.T", "1", "c1", 101L));
        assertEquals("U", upd.get("op_type").asText(), "UPDATE op_type=U");
        assertFalse(upd.get("before").isNull(), "UPDATE before 应为业务镜像");
        assertFalse(upd.get("after").isNull(), "UPDATE after 应为业务镜像");
        assertEquals(upd.get("before").get("pk").asLong(), upd.get("after").get("pk").asLong(),
                "UPDATE before/after 主键应一致（同一行变更）");

        // DELETE：before=业务镜像, after=null
        WideRecord delete = factory.next();
        while (delete.type != RecordType.DELETE) {
            delete = factory.next();
        }
        JsonNode del = MAPPER.readTree(delete.toJson("db.T", "1", "c1", 102L));
        assertEquals("D", del.get("op_type").asText(), "DELETE op_type=D");
        assertFalse(del.get("before").isNull(), "DELETE before 应为业务镜像");
        assertTrue(del.get("after").isNull(), "DELETE after 应为 null");
        assertTrue(del.get("before").has("pk"), "DELETE before 镜像应含 pk");

        System.out.println("✅ OGG-JSON 信封格式验证通过");
    }
}
