package org.javaup.config;

import cn.hutool.core.date.DateTime;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料
 * @description: 自定义json序列化
 * @author: 阿星不是程序员
 **/
public class JsonCustomSerializer extends BeanSerializerModifier {

	@Override
	public List<BeanPropertyWriter> changeProperties(SerializationConfig config, BeanDescription beanDesc,
			List<BeanPropertyWriter> beanProperties) {
		for (BeanPropertyWriter writer : beanProperties) {
			com.fasterxml.jackson.databind.JsonSerializer<Object> js = judgeType(writer);
			if (js != null) {
				writer.assignNullSerializer(js);
			}
		}
		return beanProperties;
	}
	// 方法功能：为返回对象的字段追加自定义空值序列化器。

	public com.fasterxml.jackson.databind.JsonSerializer<Object> judgeType(BeanPropertyWriter writer) {
		JavaType javaType = writer.getType();
		Class<?> clazz = javaType.getRawClass();
		if (String.class.isAssignableFrom(clazz)) {
			return new com.fasterxml.jackson.databind.JsonSerializer<Object>() {
				@Override
				public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers)
						throws IOException {
					gen.writeString("");
				}
				// 方法功能：将对应类型的空值序列化为约定的 JSON 默认值。
			};
		}
		if (Number.class.isAssignableFrom(clazz)) {
			return new com.fasterxml.jackson.databind.JsonSerializer<Object>() {
				@Override
				public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers)
						throws IOException {
					gen.writeString("");
				}
				// 方法功能：将对应类型的空值序列化为约定的 JSON 默认值。
			};
		}
		if (Boolean.class.isAssignableFrom(clazz)) {
			return new com.fasterxml.jackson.databind.JsonSerializer<Object>() {
				@Override
				public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers)
						throws IOException {
					gen.writeBoolean(false);
				}
				// 方法功能：将对应类型的空值序列化为约定的 JSON 默认值。
			};
		}
		if (java.util.Date.class.isAssignableFrom(clazz)) {
			return new com.fasterxml.jackson.databind.JsonSerializer<Object>() {
				@Override
				public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers)
						throws IOException {
					gen.writeString("");
				}
				// 方法功能：将对应类型的空值序列化为约定的 JSON 默认值。
			};
		}
		if (clazz.equals(DateTime.class)) {
			return new com.fasterxml.jackson.databind.JsonSerializer<Object>() {
				@Override
				public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers)
						throws IOException {
					gen.writeString("");
				}
				// 方法功能：将对应类型的空值序列化为约定的 JSON 默认值。
			};
		}
		if (clazz.isArray() || clazz.equals(List.class) || clazz.equals(Set.class)) {
			return new com.fasterxml.jackson.databind.JsonSerializer<Object>() {
				@Override
				public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers)
						throws IOException {
					gen.writeStartArray();
					gen.writeEndArray();
				}
				// 方法功能：将对应类型的空值序列化为约定的 JSON 默认值。
			};
		}
		return null;
	}
	// 方法功能：根据字段类型选择对应的空值 JSON 序列化策略。
}
