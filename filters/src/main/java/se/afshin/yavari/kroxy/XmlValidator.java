package se.afshin.yavari.kroxy;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import org.xml.sax.SAXException;

public class XmlValidator {
    private final Schema schema;

    public XmlValidator(String xsd) throws SAXException {
        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        // Disable external entity access to prevent XXE
        factory.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        this.schema = factory.newSchema(new StreamSource(new StringReader(xsd)));
    }

    public ValidationResult validate(byte[] xmlBytes) {
        try {
            Validator validator = schema.newValidator();
            validator.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            validator.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
            validator.validate(new StreamSource(new ByteArrayInputStream(xmlBytes)));
            return ValidationResult.valid();
        } catch (SAXException e) {
            return ValidationResult.invalid(e.getMessage());
        } catch (IOException e) {
            return ValidationResult.invalid("IO error during validation: " + e.getMessage());
        }
    }
}
