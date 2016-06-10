package macrobase.conf;

import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import org.junit.*;

import java.io.File;
import static org.junit.Assert.*;

public class MacroBaseNestedConfTest {
    @Test
    public void testKDEFields() throws Exception {
        ConfigurationFactory<MacroBaseConf> cfFactory = new ConfigurationFactory<>(
                MacroBaseConf.class,
                null,
                Jackson.newObjectMapper(),
                ""
        );
        MacroBaseConf conf = cfFactory.build(
                new File("conf/local_treekde.yaml")
        );
        assertEquals("gaussian", conf.kdeConf.kernel);
        assertEquals(.2, conf.kdeConf.tolMultiplier, 1e-10);
    }
}
