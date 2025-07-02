package org.hdf5javalib.maydo.hdfjava;

import java.math.BigInteger;
import java.util.NoSuchElementException;

public class Main {

    private static void printSeparator(String title) {
        System.out.println("\n==================================================");
        System.out.println("    " + title);
        System.out.println("==================================================");
    }

    public static void main(String[] args) {
        HdfBTree btree = new HdfBTree(2);

        // ---------------------------------
        // TEST 1: Basic Root-Level Insertion
        // ---------------------------------
        printSeparator("TEST 1: Root-Level Insertion");
        try {
            System.out.println("Inserting 'users' (Group) and 'system.conf' (Data)...");
            btree.insert(new HdfGroupObject("users"));
            btree.insert(new HdfDatasetObject("system.conf", BigInteger.valueOf(1024)));

            // UPDATED: Using string paths
            HdfDataObject users = btree.findByPath("/users");
            HdfDataObject sysConf = btree.findByPath("/system.conf");

            System.out.println("Found: " + users);
            System.out.println("Found: " + sysConf);

            if (!(users instanceof HdfGroupObject)) {
                System.out.println("FAIL: 'users' should be a HdfGroupObject.");
            } else {
                System.out.println("PASS: 'users' is a HdfGroupObject.");
            }
        } catch (Exception e) {
            System.out.println("FAIL: An exception occurred during basic insertion.");
            e.printStackTrace();
        }

        // -----------------------------------
        // TEST 2: Hierarchical Insertion
        // -----------------------------------
        printSeparator("TEST 2: Hierarchical Insertion");
        try {
            System.out.println("Inserting data into the 'users' group...");
            // UPDATED: Using string path for the parent
            btree.insert("/users", new HdfDatasetObject("alice", BigInteger.valueOf(100)));
            btree.insert("/users", new HdfDatasetObject("bob", BigInteger.valueOf(101)));

            // UPDATED: Using full string paths
            HdfDataObject alice = btree.findByPath("/users/alice");
            HdfDataObject bob = btree.findByPath("/users/bob");

            System.out.println("Found via path: " + alice);
            System.out.println("Found via path: " + bob);
            System.out.println("PASS: Correctly inserted and found objects in a group.");

        } catch (Exception e) {
            System.out.println("FAIL: An exception occurred during hierarchical insertion.");
            e.printStackTrace();
        }

        // ----------------------------------------------------
        // TEST 3: Triggering a Split in a Child Node
        // ----------------------------------------------------
        printSeparator("TEST 3: Triggering a Split in a Child Node");
        try {
            System.out.println("The 'users' child node has [alice, bob]. Max is 3. Inserting 'charlie' to fill it.");
            btree.insert("/users", new HdfDatasetObject("charlie", BigInteger.valueOf(102)));

            System.out.println("Inserting 'david' into 'users', which should cause a split.");
            btree.insert("/users", new HdfDatasetObject("david", BigInteger.valueOf(103)));

            System.out.println("Verifying the new structure after split...");

            // The median key 'bob' should be promoted to the root node.
            HdfDataObject promotedBob = btree.findByPath("/bob");
            System.out.println("Found at root: " + promotedBob);
            System.out.println("PASS: Median key 'bob' was correctly promoted to the root.");

            HdfDataObject alice = btree.findByPath("/users/alice");
            HdfDataObject charlie = btree.findByPath("/users/charlie");
            HdfDataObject david = btree.findByPath("/users/david");

            System.out.println("Found in 'users' post-split: " + alice.getObjectName() + ", " + charlie.getObjectName() + ", " + david.getObjectName());
            System.out.println("PASS: Remaining objects are still accessible in the original group.");

        } catch (Exception e) {
            System.out.println("FAIL: An exception occurred during child node split test.");
            e.printStackTrace();
        }

        // -----------------------------------
        // TEST 4: Error Handling and Root Path
        // -----------------------------------
        printSeparator("TEST 4: Error Handling and Root Path");
        // NEW: Test finding the root path
        try {
            System.out.print("Searching for root path '/'... ");
            HdfDataObject rootGroup = btree.findByPath("/");
            if (rootGroup instanceof HdfGroupObject && rootGroup.getObjectName().equals("/")) {
                System.out.println("PASS: Correctly returned a virtual root HdfGroupObject.");
            } else {
                System.out.println("FAIL: Did not return the expected root object.");
            }
        } catch (Exception e) {
            System.out.println("FAIL: Exception when finding root path.");
            e.printStackTrace();
        }

        // Test finding a non-existent path
        try {
            System.out.print("Searching for non-existent path '/users/zara'... ");
            btree.findByPath("/users/zara");
            System.out.println("FAIL: Should have thrown NoSuchElementException.");
        } catch (NoSuchElementException e) {
            System.out.println("PASS: Correctly threw NoSuchElementException.");
        }

        // Test inserting into a DataObject
        try {
            System.out.print("Inserting into '/system.conf' (a HdfDatasetObject)... ");
            btree.insert("/system.conf", new HdfDatasetObject("sub_setting", BigInteger.ZERO));
            System.out.println("FAIL: Should have thrown an exception.");
        } catch (IllegalArgumentException | NoSuchElementException e) {
            System.out.println("PASS: Correctly threw an appropriate exception.");
        }

        // -----------------------------------
        // TEST 5: DELETION
        // -----------------------------------
        printSeparator("TEST 5: DELETION");
        try {
            System.out.println("Attempting to remove '/system.conf' from the root...");
            btree.remove("/system.conf");
            System.out.print("Searching for '/system.conf' again... ");
            btree.findByPath("/system.conf");
            System.out.println("FAIL: '/system.conf' was found after being removed.");
        } catch (NoSuchElementException e) {
            System.out.println("PASS: Correctly threw NoSuchElementException, meaning the object is gone.");
        }
        try {
            System.out.println("\nAttempting to remove '/users/alice'...");
            btree.remove("/users/alice");
            System.out.print("Searching for '/users/alice' again... ");
            btree.findByPath("/users/alice");
            System.out.println("FAIL: '/users/alice' was found after being removed.");
        } catch (NoSuchElementException e) {
            System.out.println("PASS: Correctly threw NoSuchElementException, meaning the object is gone.");
        }
    }
}