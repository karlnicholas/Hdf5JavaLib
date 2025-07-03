package org.hdf5javalib.maydo.hdfjava;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

/**
 * A B-Tree that organizes BTreeObjects in a hierarchical, file-system-like structure.
 * It uses string paths (e.g., "/users/alice") for all public operations.
 */
public class HdfBTree {

    private final int groupInternalNodeK;
    private HdfBTreeNode root;

    public HdfBTree(int groupInternalNodeK) {
        if (groupInternalNodeK < 2) {
            throw new IllegalArgumentException("B-Tree minimum degree must be at least 2.");
        }
        this.groupInternalNodeK = groupInternalNodeK;
        this.root = new HdfBTreeNode(groupInternalNodeK);
    }

    // ===================================================================
    // NEW PUBLIC API (Using String paths)
    // ===================================================================

    /**
     * Finds an object by its hierarchical string path.
     * The path "/" returns a virtual HdfGroupObject representing the root.
     *
     * @param path The full path, starting with "/". Example: "/users/alice".
     * @return The found HdfDataObject.
     * @throws NoSuchElementException if the path or any of its segments do not exist.
     */
    public HdfDataObject findByPath(String path) {
        String[] pathArray = parsePath(path);
        return findByPathArray(pathArray);
    }

    /**
     * Inserts an object at the root level of the tree.
     *
     * @param obj The object to insert.
     */
    public void insert(HdfDataObject obj) {
        Objects.requireNonNull(obj, "Cannot insert a null object.");
        String[] emptyPath = new String[0];
        insertAtPathArray(emptyPath, obj);
    }

    /**
     * Inserts an object under a specific parent group, identified by its string path.
     *
     * @param parentPathStr The path to the parent group, e.g., "/users". Use "/" for the root.
     * @param obj           The object to insert.
     */
    public void insert(String parentPathStr, HdfDataObject obj) {
        String[] parentPath = parsePath(parentPathStr);
        insertAtPathArray(parentPath, obj);
    }

    /**
     * Removes an object identified by its hierarchical string path.
     *
     * @param path The full path to the object to remove, e.g., "/users/alice".
     */
    public void remove(String path) {
        String[] pathArray = parsePath(path);
        if (pathArray.length == 0) {
            throw new IllegalArgumentException("Cannot remove the root directory ('/').");
        }
        removeAtPathArray(pathArray);
    }


    // ===================================================================
    // PRIVATE IMPLEMENTATION (Using String[] arrays)
    // ===================================================================

    /**
     * Parses a user-facing string path into an internal string array for processing.
     */
    private String[] parsePath(String path) {
        if (path == null || path.trim().isEmpty() || !path.startsWith("/")) {
            throw new IllegalArgumentException("Path must be non-null and start with '/'. Invalid path: '" + path + "'");
        }
        if (path.equals("/")) {
            return new String[0]; // Special case for the root path
        }
        // Split and remove the initial empty string caused by the leading '/'
        return Arrays.copyOfRange(path.split("/"), 1, path.split("/").length);
    }

    /**
     * Private implementation for finding an object using a parsed path array.
     */
    private HdfDataObject findByPathArray(String[] path) {
        if (path == null || path.length == 0) {
            return null;
        }
        return findRecursive(root, path, 0);
    }

    /**
     * The core recursive find logic.
     */
    private HdfDataObject findRecursive(HdfBTreeNode node, String[] path, int level) {
        HdfDataObject obj = node.findObjectByName(path[level]);
        if (obj == null) {
            throw new NoSuchElementException("Path segment not found: '" + path[level] + "'");
        }

        if (level == path.length - 1) {
            return obj; // Found the target object at the end of the path.
        }

        if (obj instanceof HdfGroupObject) {
            HdfGroupObject group = (HdfGroupObject) obj;
            if (group.getChildren().isEmpty()) {
                throw new NoSuchElementException("Path continues, but group '" + group.getObjectName() + "' is empty.");
            }
            // After a split, a group can have multiple children. Search in all of them.
            for (HdfBTreeNode childNode : group.getChildren()) {
                try {
                    return findRecursive(childNode, path, level + 1);
                } catch (NoSuchElementException e) {
                    // This child didn't contain the next path segment. Continue to the next child.
                }
            }
            throw new NoSuchElementException("Path segment not found: '" + path[level + 1] + "' in any child of " + group.getObjectName());
        } else {
            throw new NoSuchElementException("Path continues, but '" + obj.getObjectName() + "' is not a group.");
        }
    }

    /**
     * Private implementation for inserting an object using a parsed path array.
     */
    private void insertAtPathArray(String[] parentPath, HdfDataObject obj) {
        Objects.requireNonNull(obj, "Cannot insert a null object.");

        if (parentPath == null || parentPath.length == 0) {
            // This is an insertion at the root level.
            HdfBTreeNode r = this.root;
            if (r.isFull()) {
                HdfBTreeNode newRoot = new HdfBTreeNode(groupInternalNodeK);
                this.root = newRoot;
                // This is a complex case. For this model, we'll create a new HdfGroupObject to hold the old root.
                // A better design might be needed for robust root splitting.
                HdfGroupObject placeholderGroup = new HdfGroupObject(r.entries.get(0).getObjectName(), null); // Placeholder
                placeholderGroup.getChildren().add(r);
                newRoot.entries.add(placeholderGroup);
                splitChild(newRoot, r, 0);
                insertNonFull(newRoot, obj);
            } else {
                insertNonFull(r, obj);
            }
            return;
        }

        // Find the parent group and the node it lives in.
        HdfDataObject groupObject = findByPathArray(parentPath);
        if (!(groupObject instanceof HdfGroupObject)) {
            throw new IllegalArgumentException("Parent path does not point to a HdfGroupObject: " + String.join("/", parentPath));
        }
        HdfGroupObject group = (HdfGroupObject) groupObject;

        // Find the node that CONTAINS the parent group.
        String[] containerPath = Arrays.copyOf(parentPath, parentPath.length - 1);
        HdfBTreeNode parentContainerNode = (containerPath.length == 0) ? this.root : findNodeAtPath(this.root, containerPath, 0);

        // Find the child node of the group where we need to insert.
        HdfBTreeNode targetNode = findAndGetChildNodeForInsert(group, obj);
        if (targetNode.isFull()) {
            splitChildOfGroup(parentContainerNode, group, targetNode);
            targetNode = findAndGetChildNodeForInsert(group, obj);
        }
        insertNonFull(targetNode, obj);
    }

    private void insertNonFull(HdfBTreeNode node, HdfDataObject obj) {
        int i = node.entries.size() - 1;
        if (node.isLeaf()) {
            while (i >= 0 && obj.compareTo(node.entries.get(i)) < 0) {
                i--;
            }
            node.entries.add(i + 1, obj);
        } else { // It's an internal node
            while (i >= 0 && obj.compareTo(node.entries.get(i)) < 0) {
                i--;
            }
            i++;
            HdfGroupObject group = (HdfGroupObject) node.entries.get(i);
            HdfBTreeNode childToDescend = findAndGetChildNodeForInsert(group, obj);

            if (childToDescend.isFull()) {
                splitChild(node, childToDescend, i);
                if (obj.compareTo(node.entries.get(i)) > 0) {
                    i++;
                }
            }
            HdfGroupObject finalGroup = (HdfGroupObject) node.entries.get(i);
            HdfBTreeNode finalChild = findAndGetChildNodeForInsert(finalGroup, obj);
            insertNonFull(finalChild, obj);
        }
    }

    /**
     * Private implementation for removing an object using a parsed path array.
     */
    private void removeAtPathArray(String[] path) {
        String[] containerPath = Arrays.copyOf(path, path.length - 1);
        HdfBTreeNode targetNode;

        if (containerPath.length == 0) {
            targetNode = this.root;
        } else {
            HdfDataObject groupObject = findByPathArray(containerPath);
            if (!(groupObject instanceof HdfGroupObject)) {
                throw new NoSuchElementException("Container path does not point to a group: " + String.join("/", containerPath));
            }
            targetNode = findNodeContainingObject((HdfGroupObject) groupObject, path[path.length - 1]);
        }

        if (targetNode == null) {
            throw new NoSuchElementException("Object not found for removal: " + String.join("/", path));
        }

        String objectNameToRemove = path[path.length - 1];
        boolean removed = targetNode.entries.removeIf(obj -> obj.getObjectName().equals(objectNameToRemove));

        if (!removed) {
            throw new NoSuchElementException("Object not found for removal: " + objectNameToRemove);
        }
        // A full B-Tree implementation would now check if `targetNode` is under-full
        // and trigger a merge or key redistribution with siblings.
    }


    // ===================================================================
    // PRIVATE HELPER METHODS (Unchanged logic)
    // ===================================================================

    private void splitChild(HdfBTreeNode parentNode, HdfBTreeNode childToSplit, int childIndexInParent) {
        HdfBTreeNode newSibling = new HdfBTreeNode(groupInternalNodeK);
        int k = this.groupInternalNodeK;
        HdfDataObject medianObject = childToSplit.entries.get(k - 1);
        for (int j = 0; j < k - 1; j++) {
            newSibling.entries.add(childToSplit.entries.remove(k));
        }
        childToSplit.entries.remove(k - 1);
        HdfGroupObject parentGroup = (HdfGroupObject) parentNode.entries.get(childIndexInParent);
        parentGroup.getChildren().add(newSibling);
        parentNode.entries.add(childIndexInParent + 1, medianObject);
    }

    private void splitChildOfGroup(HdfBTreeNode parentNode, HdfGroupObject ownerGroup, HdfBTreeNode childToSplit) {
        int k = this.groupInternalNodeK;
        HdfBTreeNode newSibling = new HdfBTreeNode(groupInternalNodeK);
        HdfDataObject medianObject = childToSplit.entries.get(k - 1);
        for (int j = 0; j < k - 1; j++) {
            newSibling.entries.add(childToSplit.entries.remove(k));
        }
        childToSplit.entries.remove(k - 1);
        ownerGroup.getChildren().add(newSibling);
        int ownerIndex = parentNode.entries.indexOf(ownerGroup);
        parentNode.entries.add(ownerIndex + 1, medianObject);
    }

    private HdfBTreeNode findNodeAtPath(HdfBTreeNode startNode, String[] path, int level) {
        if (level >= path.length) return startNode;
        HdfDataObject od = startNode.findObjectByName(path[level]);
        if (od == null) return null;
        if (level == path.length - 1) return startNode;
        if (od instanceof HdfGroupObject) {
            HdfGroupObject group = (HdfGroupObject) od;
            for (HdfBTreeNode childNode : group.getChildren()) {
                HdfBTreeNode foundNode = findNodeAtPath(childNode, path, level + 1);
                if (foundNode != null) return foundNode;
            }
        }
        return null;
    }

    private HdfBTreeNode findAndGetChildNodeForInsert(HdfGroupObject group, HdfDataObject objToInsert) {
        if (group.getChildren().isEmpty()) {
            group.getChildren().add(new HdfBTreeNode(groupInternalNodeK));
        }
        for (HdfBTreeNode child : group.getChildren()) {
            if (!child.isFull()) {
                if (child.entries.isEmpty()) return child;
                if (objToInsert.compareTo(child.entries.get(child.entries.size() - 1)) > 0) continue;
                return child;
            }
        }
        return group.getChildren().get(group.getChildren().size() - 1);
    }

    private HdfBTreeNode findNodeContainingObject(HdfGroupObject group, String objectName) {
        for(HdfBTreeNode child : group.getChildren()) {
            if (child.findObjectByName(objectName) != null) return child;
        }
        return null;
    }

    public Optional<HdfDataObject> findObjectByName(String currentComponent, HdfGroup hdfGroup) {
        return Optional.empty();
    }
}