/*
 *  This file is part of the Heritrix web crawler (crawler.archive.org).
 *
 *  Licensed to the Internet Archive (IA) by one or more individual 
 *  contributors. 
 *
 *  The IA licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.archive.modules.recrawl;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Map;
import java.util.Map.Entry;

import org.archive.bdb.BdbModule;
import org.archive.spring.ConfigPath;

import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.Database;

/**
 * @contributor kenji
 */
public class HistoryDump {
    /**
     * @param args
     * @throws FileNotFoundException 
     */
    public static void main(String[] args) throws FileNotFoundException {
        if (args.length < 1) {
            System.err.println("Speicify path to history BDB directory.");
            System.exit(1);
        }
        PrintStream out = System.out;
        if (args.length > 1) {
            out = new PrintStream(new FileOutputStream(args[1]));
        }
        String bdbdir = args[0];
        BdbModule bdb = new BdbModule();
        bdb.setDir(new ConfigPath("bdbdir", bdbdir));
        bdb.start();
        PersistLoadProcessor loader = new PersistLoadProcessor();
        
        StoredSortedMap<String, Map> store;
        StoredClassCatalog classCatalog = bdb.getClassCatalog();
        BdbModule.BdbConfig dbConfig = PersistProcessor.HISTORY_DB_CONFIG;
        Database histdb = bdb.openDatabase(loader.getHistoryDbName(), dbConfig, true);
        store = new StoredSortedMap<String, Map>(histdb, new StringBinding(),
                new SerialBinding<Map>(classCatalog, Map.class), true);

        for (Entry<String, Map> ent : store.entrySet()) {
            String surt = ent.getKey();
            Map<String, Object> map = ent.getValue();
            out.println(surt);
            for (Entry<String, Object> pent : map.entrySet()) {
                out.println("  " + pent.getKey() + "=" + pent.getValue().toString());
                if (pent.getKey().equals(RecrawlAttributeConstants.A_FETCH_HISTORY)) {
                    Map[] fetchhist = (Map[])pent.getValue();
                    for (int i = 0; i < fetchhist.length; i++) {
                        Map<?,?> fetch = fetchhist[i];
                        out.println("    [" + i + "]" + fetch);
                        if (fetch != null) {
                            for (Entry<?,?> hent : fetch.entrySet()) {
                                out.println("      " + hent.getKey() + "=" + hent.getValue());
                            }
                        }
                    }
                }
            }
        }
        
        bdb.stop();
    }
}
