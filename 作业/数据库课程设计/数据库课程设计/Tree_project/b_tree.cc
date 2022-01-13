#include "b_tree.h"
#include "thread_pool.h"

// -----------------------------------------------------------------------------
//  BTree: b-tree to index hash values produced by qalsh
// -----------------------------------------------------------------------------
BTree::BTree()						// default constructor
{
	root_     = -1;
	file_     = NULL;
	root_ptr_ = NULL;
}

// -----------------------------------------------------------------------------
BTree::~BTree()						// destructor
{
	char *header = new char[file_->get_blocklength()];
	write_header(header);			// write <root_> to <header>
	file_->set_header(header);		// write back to disk
	delete[] header; header = NULL;

	if (root_ptr_ != NULL) {
		delete root_ptr_; root_ptr_ = NULL;
	}
	if (file_ != NULL) {
		delete file_; file_ = NULL;
	}
}

// -----------------------------------------------------------------------------
void BTree::init(					// init a new tree
	int   b_length,						// block length
	const char *fname)					// file name
{
	FILE *fp = fopen(fname, "r");
	if (fp) {						// check whether the file exist
		fclose(fp);					// ask whether replace?
		// printf("The file \"%s\" exists. Replace? (y/n)", fname);

		// char c = getchar();			// input 'Y' or 'y' or others
		// getchar();					// input <ENTER>
		// assert(c == 'y' || c == 'Y');
		remove(fname);				// otherwise, remove existing file
	}			
	file_ = new BlockFile(b_length, fname); // b-tree stores here

	// -------------------------------------------------------------------------
	//  init the first node: to store <blocklength> (page size of a node),
	//  <number> (number of nodes including both index node and leaf node), 
	//  and <root> (address of root node)
	// -------------------------------------------------------------------------
	root_ptr_ = new BIndexNode();
	root_ptr_->init(0, this);
	//返回BIndexNode中的变量block_
	root_ = root_ptr_->get_block();
	//释放root-ptr的内存
	delete_root();
}

// -----------------------------------------------------------------------------
void BTree::init_restore(			// load the tree from a tree file
	const char *fname)					// file name
{
	FILE *fp = fopen(fname, "r");	// check whether the file exists
	if (!fp) {
		printf("tree file %s does not exist\n", fname);
		exit(1);
	}
	fclose(fp);

	// -------------------------------------------------------------------------
	//  it doesn't matter to initialize blocklength to 0.
	//  after reading file, <blocklength> will be reinitialized by file.
	// -------------------------------------------------------------------------
	file_ = new BlockFile(0, fname);
	root_ptr_ = NULL;

	// -------------------------------------------------------------------------
	//  read the content after first 8 bytes of first block into <header>
	// -------------------------------------------------------------------------
	char *header = new char[file_->get_blocklength()];
	file_->read_header(header);		// read remain bytes from header
	read_header(header);			// init <root> from <header>

	delete[] header; header = NULL;
}

int merge_size = 115000; //并行读取table分割数目，115 * 1000，1000个叶子结点
int num_task = 0;        //线程数目
int wait = 0;			 //用于线程暂停
int level = 0;           //当前结点层数
int node_num;            //结点数目
int last_start_block;	 //build b-tree level by level
int last_end_block;	     //build b-tree level by level

//并行读取table，写入叶子结点中
void* task_leaf_node(void *arg1, void *arg2, void *arg3, void *arg4){
	Result *table = (Result*)arg1;    
	BLeafNode **LeafArray = (BLeafNode**) arg2;  //叶子结点数组
	int *numptr = (int *)arg3;
    int end = *numptr;;
	int start = end - merge_size;
	if(end == 1000000){
		start = merge_size * (num_task - 1);
	}
	
	int   id    = -1;
	float key   = MINREAL;
	int thread = (start / merge_size); 
	int j = thread * (merge_size / 115);

	//数据写入叶子结点
	for(int i = start; i < end; i++){
		if(i % 115 == 0){
			continue;
		}
		//获取table中的id和value值
		id  = table[i].id_;
		key = table[i].key_;
		//如果当前叶子结点已满，进入下一个
		if(LeafArray[j]->isFull()){
			j++;
		}
		LeafArray[j]->add_new_child(id, key);
	}
	//任务结束，wait自增
	wait++;
	return NULL;
}

//并行读取儿子结点，写入索引结点中
void* task_index_node(void *arg1, void *arg2, void *arg3, void *arg4){
	BIndexNode **IndexArray = (BIndexNode**) arg1;  //索引结点数组
	int *numptr = (int *)arg2;
    int start = *numptr;          
	int *numptr1 = (int *)arg3;
    int end = *numptr1;

	int   block = -1;
	float key   = MINREAL;
	BLeafNode  *leaf_child = NULL;    //儿子叶子结点
	BIndexNode *index_child = NULL;   //儿子索引结点

	int j = 0;
	if(end == last_end_block){
		j = node_num / 2;
	}
	//通过block值，读入儿子结点的key值
	for (int i = start; i <= end; i++) {
		if((i - start) % 62 == 0){
			continue;
		}

		block = i;
		//获取儿子结点的key值
		if(level == 1){
			//第一层，儿子结点为叶子结点
			leaf_child = new BLeafNode();
			leaf_child->init_restore((BTree*)arg4, block);
			//获取key值
			key = leaf_child->get_key_of_node();
			if (leaf_child != NULL) {
				delete leaf_child; leaf_child = NULL;
			}
		}
		else{
			//其他层次，儿子结点为索引结点
			index_child = new BIndexNode();
			index_child->init_restore((BTree*)arg4, block);
			key = index_child->get_key_of_node(); 
			if (index_child != NULL) {
				delete index_child; index_child = NULL;
			}				
		}

		//如果当前索引结点已满，进入下一个
		if (IndexArray[j]->isFull()) {
			//printf("level:%d part:%d j:%d\n",level,part,j);
			j++;
		}
		IndexArray[j]->add_new_child(key, block);
	}
	//printf("level:%d part:%d j:%d\n",level,part,j);
	wait++;
	return NULL;
}

int BTree::bulkload(int n, const Result *table){
	int   id    = -1;
	int   block = -1;
	float key   = MINREAL;

	threadpool pool;
	num_task = (int)(n / merge_size) + 1;  //计算需要的线程数
	int max_thread = num_task + 1; 
	node_num = n / 115 + 1;    //计算需要建立的叶子结点数目
	BLeafNode **BLeafNodeArray = new BLeafNode*[node_num];  //叶子结点数组

	for(int i = 0; i < node_num; i++){
		BLeafNodeArray[i] = NULL;
	}

	//建立叶子结点，往叶子结点中添加初始数据，得到该叶子的key值，获取叶子结点block值，每115个数据创建一个叶子结点
	for(int i = 0, j = 0; i < node_num; i++, j = j + 115){
		id = table[j].id_;
		key = table[j].key_;
		BLeafNode *leaf_act_nd = NULL;
		if(!leaf_act_nd){
			leaf_act_nd = new BLeafNode();
			leaf_act_nd->init(level, this);
		}
		leaf_act_nd->add_new_child(id, key);
		BLeafNodeArray[i] = leaf_act_nd;
	}

	//获取第一个叶子结点和最后一个叶子结点的block值，用于构建索引结点
	last_start_block = BLeafNodeArray[0]->get_block();	
	last_end_block   = BLeafNodeArray[node_num - 1]->get_block();

	//初始化线程池
	threadpool_init(&pool,max_thread);
	//往线程池中添加任务
	for (int i = 0; i < num_task; i++) {
		int *arg3 = (int*)malloc(sizeof(int));
        *arg3 = merge_size * (i + 1);
		if(i == num_task - 1){
			*arg3 = n;
		}
		//将table，叶子结点数组，end，树本身传入线程函数
		threadpool_add_task(&pool, task_leaf_node, (void*)&(*table), (void*)&(*BLeafNodeArray), arg3, (void*)&(*this));
	}

	//等待所有叶子结点建立线程任务结束
	while (wait != num_task) {
		usleep(1); //sleep
	}

	//将所有叶子节点双向链接
	for(int i = 0; i < node_num - 1; i++){
		BLeafNodeArray[i + 1]->set_left_sibling(BLeafNodeArray[i]->get_block());
		BLeafNodeArray[i]->set_right_sibling(BLeafNodeArray[i + 1]->get_block());
	}

	//删除所有叶子结点，调用析构函数，写入文件
	for (int i = 0; i < node_num; i++) {
		delete BLeafNodeArray[i];
		BLeafNodeArray[i] = NULL;
	}
	delete[] BLeafNodeArray;
	BLeafNodeArray = NULL;
	level = 1;
	
	
	//叶子结点构建完成，开始构建索引结点，当start_block = end_block时
	//说明此时结点只剩下一个根节点，不再需要构建索引结点，循环结束
	while(last_start_block < last_end_block){
		//每一次循环，是一层索引结点的构建完成
		block = -1;
		key = MINREAL;
		bool first_node = true;
		node_num = (last_end_block -  last_start_block) / 62 + 1;  //计算改层需要构建的索引结点数量
		
		BLeafNode  *leaf_child = NULL;
		BIndexNode *index_child = NULL;
		BIndexNode **BIndexNodeArray = new BIndexNode*[node_num];  //索引结点数组

		//构建索引结点
		for (int i = last_start_block, j = 0; i <= last_end_block; i = i + 62, j++) {
			block = i;
			//根据block值，从儿子结点中获取key值
			if(level == 1){
				//第一层，儿子结点为叶子结点
				leaf_child = new BLeafNode();
				leaf_child->init_restore(this, block);
				//获取key值
				key = leaf_child->get_key_of_node();
				if (leaf_child != NULL) {
					delete leaf_child; leaf_child = NULL;
				}
			}
			else{
				//其他层次，儿子结点为索引结点
				index_child = new BIndexNode();
				index_child->init_restore(this, block);
				key = index_child->get_key_of_node(); 
				if (index_child != NULL){
					delete index_child; index_child = NULL;
				}				
			}

			//生成索引结点，进行初始化和添加数据
			BIndexNodeArray[j] = new BIndexNode();
			BIndexNodeArray[j]->init(level,this);
			BIndexNodeArray[j]->add_new_child(key, block);
		}
		
		wait = 0;
		num_task = 2;  //由于索引结点数目较少，所以选择两个线程进行同时构建即可
		int start = last_start_block;    
		int	end = last_start_block + (node_num / 2) * 62 - 1;
		//如果当前为根节点，则只需要一个线程进行构建
		if(BIndexNodeArray[0]->get_block() == BIndexNodeArray[node_num - 1]->get_block()){
			num_task = 1;
			end = last_end_block;
		}
		//开始双线程并行构建索引结点
		for (int i = 0; i < num_task; i++) {
			int *arg2 = (int*)malloc(sizeof(int));
			*arg2 = start;
			int *arg3 = (int*)malloc(sizeof(int));
			*arg3 = end;
			start = end + 1;
			end = last_end_block;
			printf("level:%d start:%d end:%d\n",level,*arg2,*arg3);
			//将索引结点数组，树本身，start和end传入线程函数
			threadpool_add_task(&pool, task_index_node, (void*)&(*BIndexNodeArray), arg2 , arg3, (void*)&(*this));
		}

		//等待该层所有索引结点建立线程任务完成
		while (wait != num_task) {
			usleep(1); //sleep
		}

		//将所有索引结点进行双向链接
		for(int i = 0; i < node_num - 1; i++){
			BIndexNodeArray[i + 1]->set_left_sibling(BIndexNodeArray[i]->get_block());
			BIndexNodeArray[i]->set_right_sibling(BIndexNodeArray[i + 1]->get_block());
		}

		//更新下一层的start_block和end_block值
		last_start_block = BIndexNodeArray[0]->get_block();
		last_end_block = BIndexNodeArray[node_num - 1]->get_block();
		level++;

		//删除所有索引结点，调用析构函数，写入文件
		for (int i = 0; i < node_num; i++) {
			delete BIndexNodeArray[i];
			BIndexNodeArray[i] = NULL;
		}
		delete[] BIndexNodeArray;
		BIndexNodeArray = NULL;
	}

	//线程池销毁
	threadpool_destroy(&pool);
	//更新根节点的block值
	root_ = last_end_block;		
	//printf("root:%d\n",root_);
	return 0;
}

// -----------------------------------------------------------------------------
int BTree::bulkload1(				// bulkload a tree from memory
	int   n,							// number of entries
	const Result *table)				// hash table
{ 
	//声明三个索引结点，儿子结点，上一个结点，当前结点
	BIndexNode *index_child   = NULL;
	BIndexNode *index_prev_nd = NULL;
	BIndexNode *index_act_nd  = NULL;

	//声明三个叶子结点，儿子结点，上一个结点，当前结点
	BLeafNode  *leaf_child    = NULL;
	BLeafNode  *leaf_prev_nd  = NULL;
	BLeafNode  *leaf_act_nd   = NULL;

	int   id    = -1;
	int   block = -1;
	float key   = MINREAL;

	bool first_node  = true;		// 第一个叶子节点为true
	int  start_block = 0;			// position of first node
	int  end_block   = 0;			// position of last node

	int num = 0;
	//从table中读取数据，开始建立BTree
	for (int i = 0; i < n; ++i) {
		//获取table中的id和value值
		id  = table[i].id_;
		key = table[i].key_;

		//当前叶子结点是否存在
		if (!leaf_act_nd) {
			//声明并且初始化叶子结点
			num++;
			//printf("%d\n",i);
			leaf_act_nd = new BLeafNode();
			leaf_act_nd->init(0, this);
			//printf("leaf:%d block:%d\n", num, leaf_act_nd->get_block());
			//如果当前叶子结点为第一个结点
			if (first_node) {
				first_node  = false; // init <start_block>
				//获取第一个叶子结点的block值
				start_block = leaf_act_nd->get_block();
			}
			//当前叶子结点不为第一个结点，则当前结点的左边有叶子节点
			else {					
				//对这两个叶子结点进行双向连接
				leaf_act_nd->set_left_sibling(leaf_prev_nd->get_block());
				leaf_prev_nd->set_right_sibling(leaf_act_nd->get_block());
				delete leaf_prev_nd; leaf_prev_nd = NULL;
			}
			//获取最后一个叶子结点的block值
			end_block = leaf_act_nd->get_block();
		}			
		//当前叶子结点写入数据，key值和value值				
		leaf_act_nd->add_new_child(id, key); 

	    //如果当前叶子节点存储的key值数量已经达到设置的最大值
		if (leaf_act_nd->isFull()) {
			//上一个叶子结点变为当前叶子结点
			leaf_prev_nd = leaf_act_nd;
			leaf_act_nd  = NULL;
		}
	}
	//清除指针变量
	if (leaf_prev_nd != NULL) {
		delete leaf_prev_nd; leaf_prev_nd = NULL;
	}
	if (leaf_act_nd != NULL) {
		delete leaf_act_nd; leaf_act_nd = NULL;
	}


	//所有叶子节点构建完成，接下来开始构建索引结点
	int current_level    = 1;		    // 当前索引结点的level，初始为1
	int last_start_block = start_block;	// build b-tree level by level
	int last_end_block   = end_block;	// build b-tree level by level
	int num1 = 0;
	int num2 = 0;
	int num3 = 0;

	//当最后一个结点的block值大于第一个结点的blcok值，仅剩下一个根结点结束循环
	while (last_end_block > last_start_block) {
		first_node = true; //本层第一个索引结点，为true
		//printf("level:%d start:%d end:%d\n",current_level,last_start_block,last_end_block);
		//开始构建该层索引结点
		for (int i = last_start_block; i <= last_end_block; ++i) {
			block = i;				
			//如果当前level为1时
			if (current_level == 1) {
				num1++;
				//声明儿子叶子结点，并且根据block值进行重新初始化
				leaf_child = new BLeafNode();
				leaf_child->init_restore(this, block);
				//获取key值，为该结点的第一个key值，key[0]
				key = leaf_child->get_key_of_node();
				//printf("level:%d node:%d key:%f\n",current_level,num1,key);
				delete leaf_child; leaf_child = NULL;
			}
			//如果当前level大于1时
			else {
				num2++;
				//声明儿子索引结点，并且根据block值进行重新初始化
				index_child = new BIndexNode();
				index_child->init_restore(this, block);
				//获取key值，为该结点的第一个key值，key[0]
				key = index_child->get_key_of_node();
				//printf("level:%d node:%d key:%f\n",current_level,num1,key);
				delete index_child; index_child = NULL;
			}

			//当前索引结点是否不存在
			if (!index_act_nd) {
				//声明新的当前索引结点，并且初始化
				index_act_nd = new BIndexNode();
				index_act_nd->init(current_level, this);
				//如果当前索引结点为本层索引的第一个结点
				if (first_node) {
					first_node = false;
					//获取本层第一个索引结点block值
					start_block = index_act_nd->get_block();
				}
				//如果当前索引结点不是本层索引的第一个结点
				else {
					//对这两个索引结点进行双向连接
					index_act_nd->set_left_sibling(index_prev_nd->get_block());
					index_prev_nd->set_right_sibling(index_act_nd->get_block());

					delete index_prev_nd; index_prev_nd = NULL;
				}
				//获取本层最后一个索引结点block值
				end_block = index_act_nd->get_block();
			}		
			//当前索引结点添加数据key值和block值				
			index_act_nd->add_new_child(key, block); // add new entry

			//如果当前索引结点数据已经存储满了
			if (index_act_nd->isFull()) {
				//上一个索引结点改为当前索引结点
				index_prev_nd = index_act_nd;
				index_act_nd = NULL;
			}
		}
		//清除指针变量
		if (index_prev_nd != NULL) {
			delete index_prev_nd; index_prev_nd = NULL;
		}
		if (index_act_nd != NULL) {
			delete index_act_nd; index_act_nd = NULL;
		}
		
		//本层索引结点构建完成，更新数据
		//在下次循环中，在更高层构建本层的索引结点
		last_start_block = start_block;// update info
		last_end_block = end_block;	// build b-tree of higher level
		//level等级增加
		++current_level;
	}
	//printf("level:%d start:%d end:%d\n",current_level,last_start_block,last_end_block);
	//while循环结束，只剩下一个根节点，更新根节点的block值
	root_ = last_start_block;		// update the <root>
	printf("root:%d\n",root_);
	//清除指针变量
	if (index_prev_nd != NULL) delete index_prev_nd; 
	if (index_act_nd  != NULL) delete index_act_nd;
	if (index_child   != NULL) delete index_child;
	if (leaf_prev_nd  != NULL) delete leaf_prev_nd; 
	if (leaf_act_nd   != NULL) delete leaf_act_nd; 	
	if (leaf_child    != NULL) delete leaf_child; 

	return 0;
}

// -----------------------------------------------------------------------------
void BTree::load_root() 		// load root of b-tree
{	
	if (root_ptr_ == NULL) {
		root_ptr_ = new BIndexNode();
		root_ptr_->init_restore(this, root_);
	}
}

// -----------------------------------------------------------------------------
void BTree::delete_root()		// delete root of b-tree
{
	if (root_ptr_ != NULL) { delete root_ptr_; root_ptr_ = NULL; }
}
