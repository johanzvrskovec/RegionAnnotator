package org.ki.meb.geneconnector;

import java.io.InputStream;
import org.springframework.test.context.junit38.AbstractJUnit38SpringContextTests;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.mock.MockEndpoint;

/**
 * A test class to ensure we can convert Partner 1 XML input files to the
 * canonical XML output format, using XSLT.
 */
@ContextConfiguration(locations = "/XMLInputTest-dsl-context.xml")
public class XMLInputTest extends AbstractJUnit38SpringContextTests {
    @Autowired
    protected CamelContext camelContext;
    protected ProducerTemplate<Exchange> template;

    protected void setUp() throws Exception {
        super.setUp();
        template = camelContext.createProducerTemplate();
    }

    public void testXMLConversion() throws InterruptedException {
        MockEndpoint finish = MockEndpoint.resolve(camelContext, "mock:finish");
        finish.setExpectedMessageCount(1);
        InputStream in = XMLInputTest.class.getResourceAsStream("/input-customer1.xml");
        assertNotNull(in);
        template.sendBody("direct:start", in);
        MockEndpoint.assertIsSatisfied(camelContext);
        System.err.println(finish.getExchanges().get(0).getIn().getBody());
    }
}
