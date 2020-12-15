/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorRestTestCase;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.commons.rest.SecureRestClientBuilder;

public class SecureADRestIT extends AnomalyDetectorRestTestCase {
    String aliceUser = "alice";
    RestClient aliceClient;
    String bobUser = "bob";
    RestClient bobClient;
    String catUser = "cat";
    RestClient catClient;
    String dogUser = "dog";
    RestClient dogClient;

    @Before
    public void setupSecureTests() throws IOException {
        if (!isHttps())
            return;
        createIndexRole("index_all_access", "*");
        createUser(aliceUser, aliceUser, new ArrayList<>(Arrays.asList("odfe")));
        aliceClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), aliceUser, aliceUser)
            .setSocketTimeout(60000)
            .build();

        createUser(bobUser, bobUser, new ArrayList<>(Arrays.asList("odfe")));
        bobClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), bobUser, bobUser)
            .setSocketTimeout(60000)
            .build();

        createUser(catUser, catUser, new ArrayList<>(Arrays.asList("aes")));
        catClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), catUser, catUser)
            .setSocketTimeout(60000)
            .build();

        createUser(dogUser, dogUser, new ArrayList<>(Arrays.asList()));
        dogClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), dogUser, dogUser)
            .setSocketTimeout(60000)
            .build();

        createRoleMapping("anomaly_read_access", new ArrayList<>(Arrays.asList(bobUser)));
        createRoleMapping("anomaly_full_access", new ArrayList<>(Arrays.asList(aliceUser, catUser, dogUser)));
        createRoleMapping("index_all_access", new ArrayList<>(Arrays.asList(aliceUser, bobUser, catUser, dogUser)));
    }

    @After
    public void deleteUserSetup() throws IOException {
        if (!isHttps())
            return;
        aliceClient.close();
        bobClient.close();
        catClient.close();
        dogClient.close();
        deleteUser(aliceUser);
        deleteUser(bobUser);
        deleteUser(catUser);
        deleteUser(dogUser);
    }

    public void testCreateAnomalyDetectorWithWriteAccess() {
        if (!isHttps())
            return;
        try {
            // User Alice has AD full access, should be able to create a detector
            AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
            Assert.assertNotNull(aliceDetector.getDetectorId());
        } catch (IOException e) {
            Assert.assertTrue("User Alice could not create detector", false);
        }
    }

    public void testCreateAnomalyDetectorWithReadAccess() {
        if (!isHttps())
            return;
        try {
            // User Bob has AD read access, should not be able to create a detector
            AnomalyDetector bobDetector = createRandomAnomalyDetector(false, false, bobClient);
            Assert.assertNull(bobDetector.getDetectorId());
        } catch (IOException e) {
            if (!e.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/write]")) {
                Assert.assertTrue(false);
            }
        }
    }

    public void testStartDetectorWithReadAccess() {
        if (!isHttps())
            return;
        try {
            // User Bob has AD read access, should not be able to modify a detector
            AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
            Assert.assertNotNull(aliceDetector.getDetectorId());
            Response response = startAnomalyDetector(aliceDetector.getDetectorId(), bobClient);
            Assert.assertEquals(response.getStatusLine().toString(), "HTTP/1.1 500 Internal Server Error");
        } catch (IOException e) {
            if (!e.getMessage().contains("no permissions for [cluster:admin/opendistro/ad/detector/jobmanagement]")) {
                Assert.assertTrue(false);
            }
        }
    }

    public void testStartDetectorForWriteUser() {
        if (!isHttps())
            return;
        try {
            // User Alice has AD full access, should be able to modify a detector
            AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
            Assert.assertNotNull(aliceDetector.getDetectorId());
            Response response = startAnomalyDetector(aliceDetector.getDetectorId(), aliceClient);
            Assert.assertEquals(response.getStatusLine().toString(), "HTTP/1.1 200 OK");
        } catch (IOException e) {
            Assert.assertTrue("User Alice could not start detector", false);
        }
    }

    public void testFilterByDisabled() {
        if (!isHttps())
            return;
        try {
            // User Alice has AD full access, should be able to create a detector
            AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
            // User Cat has AD full access, should be able to get a detector
            AnomalyDetector detector = getAnomalyDetector(aliceDetector.getDetectorId(), catClient);
            Assert.assertEquals(aliceDetector.getDetectorId(), detector.getDetectorId());
        } catch (IOException e) {
            Assert.assertTrue("User Cat could not get detector", false);
        }
    }

    public void testGetApiFilterByEnabled() throws IOException {
        if (!isHttps())
            return;
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        try {
            enableFilterBy();
            // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
            // Alice detector
            AnomalyDetector detector = getAnomalyDetector(aliceDetector.getDetectorId(), catClient);
            Assert.assertNull(detector.getDetectorId());
        } catch (IOException e) {
            if (!e.getMessage().contains("User does not have permissions to access detector: " + aliceDetector.getDetectorId())) {
                Assert.assertTrue(false);
            }
        }
    }

    public void testStartApiFilterByEnabled() throws IOException {
        if (!isHttps())
            return;
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        try {
            enableFilterBy();
            // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
            // Alice detector
            Response response = startAnomalyDetector(aliceDetector.getDetectorId(), catClient);
            Assert.assertEquals(response.getStatusLine().toString(), "HTTP/1.1 500 Internal Server Error");
        } catch (IOException e) {
            if (!e.getMessage().contains("User does not have permissions to access detector: " + aliceDetector.getDetectorId())) {
                Assert.assertTrue(false);
            }
        }
    }

    public void testStopApiFilterByEnabled() throws IOException {
        if (!isHttps())
            return;
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        try {
            enableFilterBy();
            // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
            // Alice detector
            Response response = stopAnomalyDetector(aliceDetector.getDetectorId(), catClient);
            Assert.assertEquals(response.getStatusLine().toString(), "HTTP/1.1 500 Internal Server Error");
        } catch (IOException e) {
            if (!e.getMessage().contains("User does not have permissions to access detector: " + aliceDetector.getDetectorId())) {
                Assert.assertTrue(false);
            }
        }
    }

    public void testDeleteApiFilterByEnabled() throws IOException {
        if (!isHttps())
            return;
        // User Alice has AD full access, should be able to create a detector
        AnomalyDetector aliceDetector = createRandomAnomalyDetector(false, false, aliceClient);
        try {
            enableFilterBy();
            // User Cat has AD full access, but is part of different backend role so Cat should not be able to access
            // Alice detector
            Response response = deleteAnomalyDetector(aliceDetector.getDetectorId(), catClient);
            Assert.assertEquals(response.getStatusLine().toString(), "HTTP/1.1 500 Internal Server Error");
        } catch (IOException e) {
            if (!e.getMessage().contains("User does not have permissions to access detector: " + aliceDetector.getDetectorId())) {
                Assert.assertTrue(false);
            }
        }
    }

    public void testCreateAnomalyDetectorWithNoBackendRole() throws IOException {
        if (!isHttps())
            return;
        try {
            enableFilterBy();
            // User Dog has AD full access, but has no backend role
            // When filter by is enabled, we block creating Detectors
            AnomalyDetector dogDetector = createRandomAnomalyDetector(false, false, dogClient);
        } catch (IOException e) {
            if (!e.getMessage().contains("Filter by backend roles is enabled and User dog does not have backend roles configured")) {
                Assert.assertTrue(false);
            }
        }
    }
}
