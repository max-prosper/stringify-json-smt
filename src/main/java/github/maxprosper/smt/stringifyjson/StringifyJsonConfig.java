package github.maxprosper.smt.stringifyjson;

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class StringifyJsonConfig extends AbstractConfig {
  public static final String SOURCE_FIELDS = "sourceFields";
  private static final String SOURCE_FIELDS_DOC = "Fields to transform containing JSON data to string. Parent-child relationships are delimited with '.' e.g. grandparent.parent.child ";

  public String[] sourceFields;

  public StringifyJsonConfig(Map<?, ?> settings) {
    super(config(), settings);

    final String fields = cleanString(getString(SOURCE_FIELDS));
    if (fields != null) {
      this.sourceFields = fields.split(",");
    }
  }

  public static ConfigDef config() {
    return new ConfigDef()
      .define(
        ConfigKeyBuilder.of(SOURCE_FIELDS, ConfigDef.Type.STRING)
          .documentation(SOURCE_FIELDS_DOC)
          .defaultValue("")
          .importance(ConfigDef.Importance.HIGH)
          .build()
      );
  }

  private static String cleanString(final String s) {
    if (s == null) {
      return s;
    }
    final String trimmed = s.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    return trimmed;
  }
}
