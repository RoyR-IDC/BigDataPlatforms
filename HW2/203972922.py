NO_ITEM = -1
OK = 0
import numpy as np 
# -------------------------------------------------------------------------------------------------------------------- #
# help functions
def update_subtree_height(x):
    x_left_child_height = -1
    if x.left:
        x_left_child_height = x.left.height
    
    x_right_child_height = -1
    if x.right:
        x_right_child_height = x.right.height

    # set 
    try:
        x.height = max([x_left_child_height, x_right_child_height]) + 1
    except:
        a=5
    return
def get_left_subtree_height(x):
    left_height = -1 
    if x.left:
        left_height = x.left.height
    return left_height
def get_right_subtree_height(x):
    right_height = -1 
    if x.right:
        right_height =  x.right.height
    return  right_height
def get_left_and_right_subtree_heights(x):
      left_height = get_left_subtree_height(x)
      right_height = get_right_subtree_height(x)
      return left_height, right_height
# -------------------------------------------------------------------------------------------------------------------- #
# Node class
class Node:
# -------------------------------------------------------------------------------------------------------------------- #
# BST class
    """
    BST Node
    """
    def __init__(self, key, value=None, left=None, right=None):
        """
        Constructor for BST Node
        :param key: int
        :param value: anything
        :param left: Left son - Node or None
        :param right: Right son - Node or None
        """
        self.key = key
        self.value = value
        self.left = left
        self.right = right
    def __repr__(self):
        return 'Node: key,value=(' + str(self.key) + ',' + str(self.value) + ')'
class BST:
    """
    BST Data Structure
    """
    def __init__(self, root=None):
        """
        Constructor for BST
        :param root: root of another BST
        """
        self.root = root
    def __level_repr(self, arr, d):
        """
        helper for repr
        """
        s = ' ' * d
        for node in arr:
            if node is None:
                s = s + '!'
            else:
                s = s + str(node.key)
            s = s + ' ' * (2 * d + 1)
        return s
    def __repr__(self):
        """
        the input is the tree and the output return a string 
        which represent the tree
        """
        s = '--------------------------------------'
        next = True
        level_arr = []
        cur_arr = [self.root]
        while next:
            next_arr = []
            for node in cur_arr:
                if node is not None:
                    next_arr.append(node.left)
                    next_arr.append(node.right)
                else:
                    next_arr.append(None)
                    next_arr.append(None)
            level_arr.append(cur_arr)
            for tmp in next_arr:
                if tmp is not None:
                    next = True
                    break
                else:
                    next = False
            cur_arr = next_arr
        d_arr = []
        d = 0
        for i in range(len(level_arr)):
            d_arr.append(d)
            d = 2 * d + 1
        d_arr.reverse()
        for i in range(len(level_arr)):
            s += '\n' + self.__level_repr(level_arr[i], d_arr[i])
        return s  
    def find_min_node_and_his_parent_node(self, node, parent):
        """
        because we working on BST The min value located in the most left node
        so we will go most left node (when his left child is None)  
        """
        if node.left is None: # get to most left child
            return node, parent
        else: # no get most left child, therefore continue to go left
            return self.find_min_node_and_his_parent_node(node.left, node)
    def find_specific_node_and_its_parent_node_using_key(self, desire_key):
        """
        defenition:
            1.  If node was not found then return None
            2.  If key is in the BST find the Node associated with key
                and its parent.
     
        :param key: int
        :return: Node if key is in BST or None o.w.
        """
        current_node = self.root # starting from the root
        current_node_parent = None # initialiezed the parent node
        while current_node is not None: # if we most left/right node and not found --> stop 
            if desire_key == current_node.key: # if is equal to the key than stop 
                return current_node, current_node_parent
            elif desire_key < current_node.key: # check the desire desire_key in the left subtree
                current_node_parent = current_node # update current node as new parent
                current_node = current_node.left # go left
            elif desire_key > current_node.key: # check the desire key in the right subtree
               current_node_parent = current_node # update current node as new parent
               current_node = current_node.right # go right
         
        return current_node, current_node_parent
    def delete_specific_node_using_key(self, node, parent):
        """
        there are 4 options:
            1. node not exists
                a) return no item
            2. node not have any children
                a) check if node is root
                b) delete the node if he is left/right child of parent
                c) update parent height relatie left/right subtree height
                   
            3. node have right subtree
                If node has only one right child then it's child will replace
                this node by becoming the child of the parent of this node.
                a) if node key greater than parent key
                   -> right subtree is right subtree of parent
                b) else -> left subtree is right subtree of parent
    
            4.  node have left subtree
                a) if node key greater than parent key
                   -> left subtree is right subtree of parent
                b) else -> left subtree is left subtree of parent
                    
            5. have left and right subtree
                *) If node has two children then replace it's value with the 
                   minimum key in the right sub-tree.
                *) then we can delete the node in the the right sub-tree.
                Notes:
        """
        # we can't delete the node because the node dosnot exist.
        node_not_exists = (node is None)
        if node_not_exists:
            return NO_ITEM
        # create all condition combinations
        node_no_left_child =  (node.left is None)
        node_no_right_child = (node.right is None)
        node_yes_right_child = (not node_no_right_child)
        node_yes_left_child = (not node_no_left_child)
        if not parent is None:
            parent_yes_left_child =  (parent.left is None)
            parent_no_right_child = (parent.right is None)
            parent_yes_left_child = (not parent_yes_left_child)
            parent_yes_right_child = (not parent_no_right_child)
        
            
        yes_right_no_left = node_yes_right_child & node_no_left_child
        yes_left_no_right = node_no_right_child & node_yes_left_child
        no_children = node_no_left_child and node_no_right_child
        if no_children:
            is_root = (parent is None)
            if is_root:   # the node is the root and we will delete him
                self.root = None
            else:
                node_is_right_child_of_parent = (parent.key < node.key)
                if node_is_right_child_of_parent:
                    parent.right = None # delete the node
                    if parent_yes_left_child: # neede to update height relative to child height
                        parent_left_height = parent.left.height
                    else: # parent becoming the root
                        parent_left_height = -1 
                    parent.height = parent_left_height + 1
                else: # node_is_left_child_of_parent
                    parent.left = None  # delete left child
                    if parent_yes_right_child: # neede to update height relative to child height
                        parent_right_height = parent.right.height
                    else: # parent becoming the root
                        parent_right_height = -1 
                    parent.height = parent_right_height + 1
            return OK
        elif yes_left_no_right:
            parent_key_is_smaller_than_node = (parent.key < node.key)
            if parent_key_is_smaller_than_node:
                parent.right = node.left
            else:
                parent.left = node.left
            return OK
        elif yes_right_no_left:
        
            parent_key_is_smaller_than_node = (parent.key < node.key)
            if parent_key_is_smaller_than_node:
                parent.right = node.right
            else:
                parent.left = node.right
            return OK
        else :
            
            right_subtree_min_descendant_node, parent_of_min_node = \
                            self.find_min_node_and_his_parent_node(node.right, node)
            # Delete the descendant using a simple only one level of recursion.
            # Because the descendant node C can't have left child.
            self.delete_specific_node_using_key(right_subtree_min_descendant_node, parent_of_min_node)
            # update key
            node.key = right_subtree_min_descendant_node.key
            return OK
        return
    def insert(self, key, value):
        """
        1. Inserts a new node is defined by pair (key,value) .
        2. if key already exists in the BST update the node's value.
        """
        # validate node is not exists in the BST
        found_node, found_parent = self.find_specific_node_and_its_parent_node_using_key(key)
        # check if node existe
        if found_node: # if so update his value
            found_node.value = value
            return
        else: # otherwise
            
            # create a new node
            new_node = Node(key, value)
            """
            doesn't found the node, however found the potential parent
            for our new node, which has at most one child.
            """
            if found_parent:
                if new_node.key < found_parent.key:
                    found_parent.left = new_node
                else:
                    found_parent.right = new_node
                return
    
            self.root = new_node
        return  
    def delete(self, key):
        """
        first of all needed to find the node we want to delete,
        secound we needed to dellete this node

        return:
                1. OK if deleted successfully 
                2. NO_ITEM if key not in the BST
        """

        found_node, found_parent = self.find_specific_node_and_its_parent_node_using_key(key)

        return self.delete_specific_node_using_key(found_node, found_parent)
    def preorder_traversal(self):
        """
        return: List of keys sorted according to the preorder traversal
        1.root
        2.left
        3.right
        """
        # if BST is empty return empty list
        if self.root is None:
            return []
        
        # intial the list with the root
        arr_traverse = [self.root.key]
        
        """
        if there is left subtree 
        set the let node as BST and call to preorder 
        """
        if self.root.left:
            left_subtree = BST(self.root.left)
            # append left subtree 
            arr_traverse += left_subtree.preorder_traversal()
        """
        if there is right subtree 
        set the let node as BST and call to preorder 
        """
        if self.root.right:
            right_subtree = BST(self.root.right)
            # append right subtree 
            arr_traverse += right_subtree.preorder_traversal()

        return arr_traverse
    def postorder_traversal(self):
        """
        return: List of keys sorted according to the postorder traversal
        1.left
        2.right
        3.root
        """
        # if BST is empty return empty list
        if self.root is None:
            return []

        # intial the list 
        arr_traverse = []
        
        """
        if there is left subtree 
        set the let node as BST and call to postorder 
        """
        if self.root.left:
            left_subtree = BST(self.root.left)
            # append left subtree 
            arr_traverse = left_subtree.postorder_traversal()
        """
        if there is right subtree 
        set the let node as BST and call to postorder 
        """
        if self.root.right:
            right_subtree = BST(self.root.right)
            # append right subtree 
            arr_traverse += right_subtree.postorder_traversal()
        
        # append in the end of list root  
        arr_traverse.append(self.root.key)

        return arr_traverse
    def inorder_traversal(self):
        """
        return: List of keys sorted according to the inorder traversal
        1.left
        2.root
        3.right
        """
        # if BST is empty return empty list
        if self.root is None:
            return []

        # intial the list 
        arr_traverse = []
        
        """
        if there is left subtree 
        set the let node as BST and call to inorder 
        """
        if self.root.left:
            left_subtree = BST(self.root.left)
            arr_traverse = left_subtree.inorder_traversal()
        
        # append in the middle of list root  
        arr_traverse.append(self.root.key)
        
        """
        if there is right subtree 
        set the let node as BST and call to inorder 
        """
        if self.root.right:
            right_subtree = BST(self.root.right)
            arr_traverse += right_subtree.inorder_traversal()

        return arr_traverse
    def find(self, key):
        """
        find a nodd using key
        """
        cur = self.root
        while cur is not None:
            if key > cur.key:
                cur = cur.right
            elif key < cur.key:
                cur = cur.left
            else:
                return cur
        return cur
    @staticmethod
    def create_BST_from_sorted_arr(arr):
        """
        Creates a balanced BST from a sorted list of keys according to the algorithm from class.
        the input is order list and the output is BST tree
        """
        # check if is empty
        if (arr is None) or (len(arr) == 0):
            return None
        
        # if there is only one element in the list
        if len(arr) == 1:
            return BST(Node(arr[0]))
        
        # get the midell element index in the list
        mid = len(arr) // 2
        
        # generte empty BST
        root_node = Node(None)
        
        # set the middle element as the root 
        root_node.key = arr[mid]
        """
        all element until the middle-1 index will be in the left subtree 
        of the root
        """
        left_subtree = BST.create_BST_from_sorted_arr(arr[:mid])
        if not left_subtree is None:
            root_node.left = left_subtree.root

        """
        all element from the middle+1 index until the end of list will 
        be in the right subtree of the root
        """
        right_subtree = BST.create_BST_from_sorted_arr(arr[(mid + 1):])
        if not right_subtree is None:
            root_node.right = right_subtree.root
        return BST(root_node)
    
# -------------------------------------------------------------------------------------------------------------------- #
# AVL Node class
class AVLNode(Node):
# -------------------------------------------------------------------------------------------------------------------- #
# AVL Node class
# -------------------------------------------------------------------------------------------------------------------- #
# AVL class
    """
    Node of AVL
    """

    def __init__(self, key, value=None, left=None, right=None):
        """
        Constructor for AVL Node
        :param key: int
        :param value: anything
        :param left: Left son - Node or None
        :param right: Right son - Node or None
        """
        super(AVLNode, self).__init__(key, value, left, right)
        self.height = 0
    def __repr__(self):
        return super(AVLNode, self).__repr__() + ',' + 'height=' + str(self.height)
    def get_balance(self):
        """
        get the blance of the tree
        """
        #initial the subtrees (left\right)
        h_left, h_right = -1, -1
        # if exists update subtree
        if self.left:
            h_left = self.left.height
        # if exists update subtree
        if self.right:
            h_right = self.right.height
        return h_left-h_right
class AVL(BST):
    """
    AVL Data Structure
    """
    def __init__(self, root=None):
        """
        Constructor for a new AVL
        :param root: root of another AVL
        """
        super(AVL, self).__init__(root)
    def right_rotate(self, node, parent, left_or_right_node_of_parent):
        """
        rotate right base lacture
        """

        # initial x to be the node  
        y = node
        
        # initial z to be the left subtree
        z = node.left
        
        # set the left subtree of y as left subtree of z 
        y.left = z.right
        
        # set the right subtree of z as y 
        z.right = y

        # if parent is the root set z to be the root
        if parent is None:
            self.root = z
        # the parent stay the parent and needed to blance his childrens 
        else:
            # if need to update rightsubtree using z
            if left_or_right_node_of_parent == AVL.NODE_IS_RIGHT_OF_ITS_PARENT:
                parent.right = z
            # if need to update leftsubtree using z
            else:
                parent.left = z

        # update subtree height after all the rottation
        update_subtree_height(y)
        update_subtree_height(z)
    def left_rotate(self, node, parent, left_or_right_node_of_parent):
        """
        rotate left base lacture
        """
        
        # initial x to be the node  
        x = node
        
        # initial y to be the right subtree
        y = node.right

        # set the right subtree of x as left subtree of y 
        x.right = y.left  
        
        # finaly set the left subtree of y to be the node  himself
        y.left = x

        # if parent is the root set y to be the root
        if parent is None:
            self.root = y
        # the parent stay the parent and needed to blance his childrens 
        else:
            # if need to update rightsubtree
            if left_or_right_node_of_parent == AVL.NODE_IS_RIGHT_OF_ITS_PARENT:
                # set the right subtree of the parent
                parent.right = y
            # if need to update leftsubtree
            else:
                parent.left = y
        
        # update subtree height after all the rottation
        update_subtree_height(x)
        update_subtree_height(y)
        return 
    def balance(self):
        """
        the input is a tree
        the function blance the tree in the case where the tree is not
        balanced
        """
        # validate that needed to blance the tree
        if abs(self.root.get_balance()) <= 1:
            return

        root_left_height, root_right_height =  get_left_and_right_subtree_heights(self.root)
        left_subtree_is_heigher = root_left_height > root_right_height
        if left_subtree_is_heigher:
            # set y to as the left subtree of the root
            y = self.root.left
            y_left_height, y_right_height =  get_left_and_right_subtree_heights(y)
            left_subtree_is_heigher = y_left_height > y_right_height
            
            if left_subtree_is_heigher:
                self.left_rotate(y, self.root, left_subtree_is_heigher)
            self.right_rotate(self.root, None, left_subtree_is_heigher)
        # right subtree is heigher
        else:
            # set y to as the right subtree of the root
            y = self.root.right
            y_left_height, y_right_height =  get_left_and_right_subtree_heights(y)
            left_subtree_is_heigher = y_left_height > y_right_height
            if left_subtree_is_heigher:
                self.right_rotate(y, self.root, left_subtree_is_heigher)
            self.left_rotate(self.root, None, left_subtree_is_heigher)            
    def insert(self, key, value, recursion_level=0):
        """
        insert a new node to AVL tree
        """
        # check node key is already in the tree
        found_node, found_parent = self.find_specific_node_and_its_parent_node_using_key(key)
        # if so update his value
        if found_node:
            found_node.value = value
            return
        # validate that  tree is not empty
        if self.root is None:
            # if so set new avl tree where the node is the root
            self.root = AVLNode(key, value)
        else:
            node_in_left_subtree = (key < self.root.key)
            if node_in_left_subtree:
                # set the left root child as AVL tree
                left_tree = AVL(self.root.left)
                # try to insert the node in the left subtree
                left_tree.insert(key, value, recursion_level+1)
                # update the left subtree
                self.root.left = left_tree.root
                root_right_height = get_left_subtree_height(self.root)

                self.root.height = max(left_tree.root.height, root_right_height) + 1
            elif key > self.root.key:
                # set the right root child as AVL tree
                right_tree = AVL(self.root.right)
                # try to insert the node in the left subtree
                right_tree.insert(key, value, recursion_level+1)
                # update the left subtree
                self.root.right = right_tree.root
                # get root left subtree heights
                root_left_height = get_left_subtree_height(self.root)
                self.root.height = max(right_tree.root.height, root_left_height) + 1
        
        # after deleting the node needed to validate the node is blanced
        self.balance()  
    def delete(self, key, recursion_level=0):
    
        """
        delete a node from AVL tree
        """
        # validate that  tree is not empty
        if self.root is None:
            return NO_ITEM
        found_node_2_delete = (key == self.root.key)
        """
        if there is no parent to this root --> delete node
        """
        if found_node_2_delete:
            self.delete_specific_node_using_key(self.root, None)
            return OK
        # find in left subtree
        node_in_left_subtree = key < self.root.key
        if node_in_left_subtree:
            # if there is left subtree
            if self.root.left:
                first_left_node_is_the_node_2_delete = (key == self.root.left.key)
                if first_left_node_is_the_node_2_delete:
                    self.delete_specific_node_using_key(self.root.left, self.root)
                # the node 2 delete in one of children of left subtree
                else:
                    # set the left root child as AVL tree
                    left_tree = AVL(self.root.left)
                    """
                    call to delete the node in the subtrees of 
                    left child to the new avl tree
                    """
                    left_tree.delete(key, recursion_level+1)
            # if there is no left root the node is not exists
            else:
                return NO_ITEM
        # node_in_right_subtree
        elif key > self.root.key:
            # if there is right subtree
            if self.root.right:
                if key == self.root.right.key:
                    self.delete_specific_node_using_key(self.root.right, self.root)
                else:
                    # set the left root child as AVL tree
                    right_tree = AVL(self.root.right)
                    """
                    call to delete the node in the subtrees of 
                    right child to the new avl tree
                    """
                    right_tree.delete(key, recursion_level+1)
                    
            # if there is no right root the node is not exists
            else:
                return NO_ITEM
        # after deleting the node needed to validate the node is blanced
        self.balance()
    def find(self, key):
        """
        find a nodd using key
        """
        cur = self.root
        while cur is not None:
            if key > cur.key:
                cur = cur.right
            elif key < cur.key:
                cur = cur.left
            else:
                return cur
        return cur
    def delete_specific_node_using_key(self, node, parent):
        """
        there are 4 options:
            1. node not exists
                a) return no item
            2. node not have any children
                a) check if node is root
                b) delete the node if he is left/right child of parent
                c) update parent height relatie left/right subtree height
                   
            3. node have right subtree
                If node has only one right child then it's child will replace
                this node by becoming the child of the parent of this node.
                a) if node key greater than parent key
                   -> right subtree is right subtree of parent
                b) else -> left subtree is right subtree of parent
    
            4.  node have left subtree
                a) if node key greater than parent key
                   -> left subtree is right subtree of parent
                b) else -> left subtree is left subtree of parent
                    
            5. have left and right subtree
                *) If node has two children then replace it's value with the 
                   minimum key in the right sub-tree.
                *) then we can delete the node in the the right sub-tree.
                Notes:
        """
        # we can't delete the node because the node dosnot exist.
        node_not_exists = (node is None)
        if node_not_exists:
            return NO_ITEM
        # create all condition combinations
        node_no_left_child =  (node.left is None)
        node_no_right_child = (node.right is None)
        node_yes_right_child = (not node_no_right_child)
        node_yes_left_child = (not node_no_left_child)
        if not parent is None:
            parent_yes_left_child =  (parent.left is None)
            parent_no_right_child = (parent.right is None)
            parent_yes_left_child = (not parent_yes_left_child)
            parent_yes_right_child = (not parent_no_right_child)
        
            
        yes_right_no_left = node_yes_right_child & node_no_left_child
        yes_left_no_right = node_no_right_child & node_yes_left_child
        no_children = node_no_left_child and node_no_right_child
        if no_children:
            is_root = (parent is None)
            if is_root:   # the node is the root and we will delete him
                self.root = None
            else:
                node_is_right_child_of_parent = (parent.key < node.key)
                if node_is_right_child_of_parent:
                    parent.right = None # delete the node
                    if parent_yes_left_child: # neede to update height relative to child height
                        parent_left_height = parent.left.height
                    else: # parent becoming the root
                        parent_left_height = -1 
                    parent.height = parent_left_height + 1
                else: # node_is_left_child_of_parent
                    parent.left = None  # delete left child
                    if parent_yes_right_child: # neede to update height relative to child height
                        parent_right_height = parent.right.height
                    else: # parent becoming the root
                        parent_right_height = -1 
                    parent.height = parent_right_height + 1
            return OK
        elif yes_left_no_right:
            parent_key_is_smaller_than_node = (parent.key < node.key)
            if parent_key_is_smaller_than_node:
                parent.right = node.left
            else:
                parent.left = node.left
            return OK
        elif yes_right_no_left:
        
            parent_key_is_smaller_than_node = (parent.key < node.key)
            if parent_key_is_smaller_than_node:
                parent.right = node.right
            else:
                parent.left = node.right
            return OK
        else :
            
            right_subtree_min_descendant_node, parent_of_min_node = \
                            self.find_min_node_and_his_parent_node(node.right, node)
            # Delete the descendant using a simple only one level of recursion.
            # Because the descendant node C can't have left child.
            self.delete_specific_node_using_key(right_subtree_min_descendant_node, parent_of_min_node)
            # update key
            node.key = right_subtree_min_descendant_node.key
            return OK
        return
# ----------------------------------BST------------------------------------------------------------------------------- #

#### #### #### tests #### #### ####
"""
tests
# test1

tree = BST()
print(tree.__repr__()) #calls __repr__ of class BST
tree.insert(10,"value for 10")
tree.insert(20,"Hi")
print(tree.__repr__()) #calls __repr__ of class BST
# -------------------------------------------------------------------------------------------------------------------- #
# test2
tree.insert(30,"Hello")
tree.insert(30,"50")
tree.insert(3,"Or")
print(tree.__repr__()) #calls __repr__ of class BST
tree.insert(9,"Or")
print(tree.__repr__()) #calls __repr__ of class BST
# -------------------------------------------------------------------------------------------------------------------- #
# test3
arr = [1,2,3,4,5,7,8,9]
tree = BST.create_BST_from_sorted_arr(arr)
print(tree.__repr__()) #calls __repr__ of class BST
# ----------------------------------AVL------------------------------------------------------------------------------- #
# test1 
tree = AVL()
tree.insert(10, "value for 10")
tree.insert(20, "Hi")
print(tree)
# -------------------------------------------------------------------------------------------------------------------- #
# test2 
tree.insert(30, "Hello")
print(tree)
# -------------------------------------------------------------------------------------------------------------------- #


keys = [20, 14, 30, -10, 17, 25, 45, 11, 35, 200, 41, 40]
values = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l']
elements = list(zip(keys,values))

my_bst = BST()
print('*********************************************')
to_delete = 14
print(f'Delete node ({to_delete}) that doesnt exit')
res = my_bst.delete(to_delete)
print('original Tree')
print(my_bst)
print(f"result for deleting {to_delete} is {res}")
print('Tree after deleting')
print(my_bst)

# fix
print('*********************************************')
to_insert = elements[0]
print(f'Insert node ({to_delete}) node which is the root')
res = my_bst.insert(to_insert[0], to_insert[1])
print(f"result for inserting {to_insert} is {res}")
print('Tree after inserting')
print(my_bst)
to_delete = 20
print(f'Delete node ({to_delete}) that is the only one')
res = my_bst.delete(to_delete)
print('original Tree')
print(my_bst)
print(f"result for deleting {to_delete} is {res}")
print('Tree after deleting')
print(my_bst)


print('*********************************************')
print('Build Tree from these elements:')
print (elements)
my_bst = BST()
for e in elements:
    my_bst.insert(e[0],e[1])
print('original Tree')
print(my_bst)


print('*********************************************')
sorted = my_bst.inorder_traversal()
print(f'Build BST from sorted arr {sorted}')
cur_bst = BST.create_BST_from_sorted_arr(sorted)
print(cur_bst)
### fix
print('*********************************************')
print(f'Build AVL from sorted same sorted array above and compare answers')
keys = sorted
values = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l']
elements = list(zip(keys,values))
my_avl = AVL()
for e in elements:
    my_avl.insert(e[0],e[1])
    print(my_avl)
print('*********************************************')




# fix
keys = [70, 60, 85, 50, 63, 77, 90, 65, 80, 88, 100, 110]
values = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l']
elements = list(zip(keys,values))
print('*********************************************')
print('Elements inserted to key by order:')
print (elements)
print('*********************************************')
my_avl = AVL()
for e in elements:
    my_avl.insert(e[0],e[1])

print('original Tree')
print(my_avl)
print('original heights:')
heights = [my_avl.find(e).height for e in my_avl.preorder_traversal()]
elements_heights = list(zip(keys,values,heights))
print(elements_heights)

# fix 
to_delete = 60
print('*********************************************')
print(f'Delete node ({to_delete}) with two children')
res = my_avl.delete(to_delete)
print('Tree after deleting:')
print(my_avl)
print(f'delete result is: {res}')
print('New heights:')
my_avl_list = my_avl.preorder_traversal()
keys = [my_avl.find(e).key for e in my_avl_list]
values = [my_avl.find(e).value for e in my_avl_list]
heights = [my_avl.find(e).height for e in my_avl_list]
elements_heights = list(zip(keys,values,heights))
print(elements_heights)
print('*********************************************')

to_delete = 88
print(f'Delete node ({to_delete}) with no children')
res = my_avl.delete(to_delete)
print('Tree after deleting:')
print(my_avl)
print(f'delete result is: {res}')
print('New heights:')
my_avl_list = my_avl.preorder_traversal()
keys = [my_avl.find(e).key for e in my_avl_list]
values = [my_avl.find(e).value for e in my_avl_list]
heights = [my_avl.find(e).height for e in my_avl_list]
elements_heights = list(zip(keys,values,heights))
print(elements_heights)
print('*********************************************')
to_delete = 77
print(f'Delete node ({to_delete}) with one child')
res = my_avl.delete(to_delete)
print('Tree after deleting:')
print(my_avl)
print(f'delete result is: {res}')
print('New heights:')
my_avl_list = my_avl.preorder_traversal()
keys = [my_avl.find(e).key for e in my_avl_list]
values = [my_avl.find(e).value for e in my_avl_list]
heights = [my_avl.find(e).height for e in my_avl_list]
elements_heights = list(zip(keys,values,heights))
print(elements_heights)
print('*********************************************')
to_insert = (64,'m')
print(f'Insert node ({to_insert[0],to_insert[1]}) that has left-right rotation path')
res = my_avl.insert(to_insert[0],to_insert[1])
print('Tree after inserting:')
print(my_avl)
print(f'insert result is: {res}')
print('New heights:')
my_avl_list = my_avl.preorder_traversal()
keys = [my_avl.find(e).key for e in my_avl_list]
values = [my_avl.find(e).value for e in my_avl_list]
heights = [my_avl.find(e).height for e in my_avl_list]
elements_heights = list(zip(keys,values,heights))
print(elements_heights)
print('*********************************************')
to_insert = (64,'jjjj')
print(f'Insert node ({to_insert[0],to_insert[1]}) that exist in the tree ')
res = my_avl.insert(to_insert[0],to_insert[1])
print('Tree after inserting:')
print(my_avl)
print(f'insert result is: {res}')
print('New heights:')
my_avl_list = my_avl.preorder_traversal()
keys = [my_avl.find(e).key for e in my_avl_list]
values = [my_avl.find(e).value for e in my_avl_list]
heights = [my_avl.find(e).height for e in my_avl_list]
elements_heights = list(zip(keys,values,heights))
print(elements_heights)
print('*********************************************')
"""
