package com.docner.hadoop.fuse.mount;

import com.docner.hadoop.fuse.HDFSFuseAdapterInitializer;
import com.docner.hadoop.fuse.mount.SingleMount;
import com.docner.util.InitializationParameters;
import com.docner.util.Initializer;
import static com.docner.util.Lookup.register;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author wiebe
 */
public class hadoopmountTest {
    
    public hadoopmountTest() {
    }

    @Test
    public void testParseMountOptionsWithSpecificUser() throws Initializer.InitializationException {
        String options = "rw,noexec,context=\"system_u:object_r:tmp_t:s0:c127,c456\",user=cvc,nosuid,nodev";
        
        HDFSFuseAdapterInitializer init = new HDFSFuseAdapterInitializer();
        InitializationParameters params = new InitializationParameters();
        register(InitializationParameters.class, params);
        
        SingleMount.parseMountOptions(options, params, init);
        
        assertEquals("cvc", params.get(init, "hdfsUser"));
    }
    @Test
    public void testParseMountOptionsWithOwner() throws Initializer.InitializationException {
        String options = "rw,noexec,context=\"system_u:object_r:removable_t\",owner,nosuid,nodev";
        
        HDFSFuseAdapterInitializer init = new HDFSFuseAdapterInitializer();
        InitializationParameters params = new InitializationParameters();
        register(InitializationParameters.class, params);
        
        SingleMount.parseMountOptions(options, params, init);
        
        assertEquals("true", params.get(init, "useStartupLoginUser"));
    }
    @Test
    public void testParseMountOptionsWithOwnerAsLogin() throws Initializer.InitializationException {
        String options = "rw,noexec,context=\"system_u:object_r:removable_t\",login,nosuid,nodev";
        
        HDFSFuseAdapterInitializer init = new HDFSFuseAdapterInitializer();
        InitializationParameters params = new InitializationParameters();
        register(InitializationParameters.class, params);
        
        SingleMount.parseMountOptions(options, params, init);
        
        assertEquals("true", params.get(init, "useStartupLoginUser"));
    }
}
