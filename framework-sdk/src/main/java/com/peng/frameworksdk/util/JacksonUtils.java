package com.peng.frameworksdk.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.peng.frameworksdk.exception.JsonOperationException;
import org.springframework.util.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class JacksonUtils {

    private static ObjectMapper objectMapper = null;

    static {
        objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        objectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.registerModule(new JavaTimeModule());
    }

    public JacksonUtils() {
    }

    public static String writeValueAsString(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException var2) {
            var2.printStackTrace();
            throw new JsonOperationException("JSON 转化异常！");
        }
    }

    public static String writeValueAsStringForNull(Object obj) {
        objectMapper.getSerializerProvider().setNullValueSerializer(new JsonSerializer<Object>() {
            public void serialize(Object arg0, JsonGenerator arg1, SerializerProvider arg2) throws IOException, JsonProcessingException {
                arg1.writeString("");
            }
        });

        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException var2) {
            var2.printStackTrace();
            throw new JsonOperationException("JSON 转化异常！");
        }
    }

    public static Map<String, Object> toMap(String json) {
        try {
            Map<String, Object> maps = (Map)objectMapper.readValue(json, Map.class);
            return maps;
        } catch (Exception var2) {
            throw new JsonOperationException("字符串转为map异常！！");
        }
    }

    public static byte[] toJsonByte(Object obj) {
        try {
            return objectMapper.writeValueAsBytes(obj);
        } catch (Exception var2) {
            throw new JsonOperationException("将对象转换为JSON字符串二进制数组错误！！");
        }
    }

    public static OutputStream toJsonOutStream(Object obj) {
        try {
            OutputStream os = new ByteArrayOutputStream();
            objectMapper.writeValue(os, obj);
            return os;
        } catch (Exception var2) {
            throw new JsonOperationException("无法转化为字符串流！！");
        }
    }

    public static <T> T toObject(Class<T> clazz, String json) {
        T obj = null;

        try {
            if (StringUtils.isEmpty(json)) {
                return null;
            } else {
                obj = objectMapper.readValue(json, clazz);
                return obj;
            }
        } catch (Exception var4) {
            throw new JsonOperationException("json字符串转化错误！！");
        }
    }

    public static <T> T toObject(Class<T> clazz, byte[] bytes) {
        T obj = null;

        try {
            if (bytes != null && bytes.length != 0) {
                obj = objectMapper.readValue(bytes, clazz);
                return obj;
            } else {
                return null;
            }
        } catch (Exception var4) {
            throw new JsonOperationException("二进制转化错误！！");
        }
    }

//    public static JSONObject toJSONObject(String json) {
//        JSONObject jsonObject = null;
//
//        try {
//            JSONUtils.getMorpherRegistry().registerMorpher(new DateMorpher(new String[]{"yyyy-MM-dd HH:mm:ss"}));
//            jsonObject = JSONObject.fromObject(json);
//            return jsonObject;
//        } catch (Exception var3) {
//            throw new JsonOperationException("json字符串转为list异常！！");
//        }
//    }

    public static <T> List<T> toObjectList(Class<T> clazz, String json) {
        try {
            JavaType javaType = objectMapper.getTypeFactory().constructParametricType(List.class, new Class[]{clazz});
            return (List)objectMapper.readValue(json, javaType);
        } catch (IOException var3) {
            var3.printStackTrace();
            throw new JsonOperationException("json字符串转为list异常！！");
        }
    }

    public static String toJsonTree(List<ConcurrentHashMap<String, Object>> pageMap, Object... count) {
        List<ConcurrentHashMap<String, Object>> myMap = new ArrayList();
        Iterator var3 = pageMap.iterator();

        while(var3.hasNext()) {
            ConcurrentHashMap<String, Object> map = (ConcurrentHashMap)var3.next();
            ConcurrentHashMap<String, Object> tempMap = new ConcurrentHashMap();

            String key;
            Object value;
            for(Iterator iterator = map.keySet().iterator(); iterator.hasNext(); tempMap.put(key.toLowerCase(), value)) {
                key = (String)iterator.next();
                value = map.get(key);
                if (key.toLowerCase().equals("parentid")) {
                    tempMap.put("_parentId", value);
                }
            }

            myMap.add(tempMap);
        }

        HashMap<String, Object> jsonMap = new HashMap();
        jsonMap.put("total", count);
        jsonMap.put("rows", myMap);

        try {
            return objectMapper.writeValueAsString(jsonMap);
        } catch (JsonProcessingException var9) {
            throw new JsonOperationException("转换json树异常！！");
        }
    }

}
