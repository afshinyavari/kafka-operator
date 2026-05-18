package se.afshin.yavari.kroxy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

import static org.assertj.core.api.Assertions.assertThat;

class XmlValidatorTest {

    private static final String ORDER_XSD = """
        <?xml version="1.0" encoding="UTF-8"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xs:element name="order">
            <xs:complexType>
              <xs:sequence>
                <xs:element name="id"     type="xs:string"/>
                <xs:element name="amount" type="xs:decimal"/>
              </xs:sequence>
            </xs:complexType>
          </xs:element>
        </xs:schema>
        """;

    private XmlValidator validator;

    @BeforeEach
    void setUp() throws SAXException {
        validator = new XmlValidator(ORDER_XSD);
    }

    @Test
    void validXmlPassesValidation() {
        byte[] xml = "<order><id>ORD-1</id><amount>99.99</amount></order>".getBytes();
        ValidationResult result = validator.validate(xml);
        assertThat(result.isValid()).isTrue();
    }

    @Test
    void missingRequiredElementFailsValidation() {
        byte[] xml = "<order><id>ORD-1</id></order>".getBytes();
        ValidationResult result = validator.validate(xml);
        assertThat(result.isValid()).isFalse();
        assertThat(result.getErrorMessage()).isNotBlank();
    }

    @Test
    void wrongRootElementFailsValidation() {
        byte[] xml = "<invoice><id>INV-1</id><amount>50</amount></invoice>".getBytes();
        ValidationResult result = validator.validate(xml);
        assertThat(result.isValid()).isFalse();
    }

    @Test
    void malformedXmlFailsValidation() {
        byte[] xml = "this is not xml at all".getBytes();
        ValidationResult result = validator.validate(xml);
        assertThat(result.isValid()).isFalse();
        assertThat(result.getErrorMessage()).isNotBlank();
    }

    @Test
    void emptyPayloadFailsValidation() {
        byte[] xml = new byte[0];
        ValidationResult result = validator.validate(xml);
        assertThat(result.isValid()).isFalse();
    }

    @Test
    void xxeAttemptIsBlocked() {
        // External entity injection should be rejected by ACCESS_EXTERNAL_DTD="" setting
        byte[] xml = ("""
            <!DOCTYPE order [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
            <order><id>&xxe;</id><amount>1</amount></order>
            """).getBytes();
        ValidationResult result = validator.validate(xml);
        assertThat(result.isValid()).isFalse();
    }
}
