#include <iostream>
#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <string>
#include <set>
#include <fstream>
#include <sstream>
#include <queue>
#include "def.h"
#include "util.h"
#include "random.h"
#include "pri_queue.h"
#include "b_node.h"
#include "b_tree.h"

using namespace std;

int id_total = 0, id_correct = 0;
char tree_file[200];
int block_length;
char root_level;
FILE *tree_fp; //*output_fp;
Result *table = new Result[1000000]; 
Result *table_copy = new Result[1000000]; 
void check_node(int block){
	char level;
	int num_entries, num_keys, left_sibling, right_sibling, son;
	float key;
	
	fseek(tree_fp, (1 + block)*block_length, SEEK_SET); // 定位到块的位置
	fread(&level, SIZECHAR, 1, tree_fp); // 通过块的level就可以判断是叶子节点还是索引节点
	
	if(level != 0){ // level != 0 时，为索引节点，1char + 3int + num_entries（51）对key和son
		fread(&num_entries,   SIZEINT, 1, tree_fp);
		fread(&left_sibling,  SIZEINT, 1, tree_fp);
		fread(&right_sibling, SIZEINT, 1, tree_fp);
		
		for(int i = 0; i < num_entries; i++){
			fread(&key, SIZEFLOAT, 1, tree_fp);
			fread(&son, SIZEINT, 1, tree_fp);
			check_node(son); // son 即子节点，继续遍历子节点
			fseek(tree_fp, (1+block)*block_length+SIZECHAR+SIZEINT*3+(i+1)*(SIZEFLOAT+SIZEINT), SEEK_SET);
		}
	}
	else if(level == 0){ // level == 0 时，为叶子节点，1char + 4int + 8key + 115id
		fread(&num_entries,   SIZEINT, 1, tree_fp);
		fread(&left_sibling,  SIZEINT, 1, tree_fp);
		fread(&right_sibling, SIZEINT, 1, tree_fp);
		fread(&num_keys,      SIZEINT, 1, tree_fp);

		float keys[num_keys]; // 8
		int ids[num_entries]; // 115

		fread(keys, SIZEFLOAT, num_keys,    tree_fp);
		fread(ids,  SIZEINT,   num_entries, tree_fp);

		for(int i = 0; i < num_entries; i++){
			if(ids[i] != 0){
				if(ids[i] == table_copy[id_total].id_) {
					id_correct++;
				}
				else printf("\n %d and %d", ids[i], table_copy[id_total].id_);
				id_total++;
			}
		}
	}
}

void check(){
	printf("\ncomparing ids...\n");
	int num_blocks;
	int root_block;

	tree_fp = fopen(tree_file, "r");
	if(!tree_fp) exit(1);

	fread(&block_length, SIZEINT, 1, tree_fp);
	fread(&num_blocks,   SIZEINT, 1, tree_fp);
	fread(&root_block,   SIZEINT, 1, tree_fp); // 得到root_block

	fseek(tree_fp, (1 + root_block) * block_length, SEEK_SET); // 定位到root
	fread(&root_level, SIZECHAR, 1, tree_fp); // 取得root的level，主要是验证有多少层
	printf(" block_length = %d\n num_blocks = %d\n root_block = %d\n root_level = %d\n", block_length, num_blocks, root_block, root_level);
	
	check_node(root_block);
	fclose(tree_fp);
	printf("\naccuracy of id = %d/%d\n", id_correct, id_total);
}

int main(int nargs, char **args){    
	char data_file[200];
	int  B_ = 512; // node size
	int n_pts_ = 1000000, option = 0;
	//int numOfThread;
	printf("输入0以串行执行，1以并行执行 : ");
	scanf("%d", &option);
	strncpy(data_file, "./data/dataset.csv", sizeof(data_file));
	strncpy(tree_file, "./result/B_tree", sizeof(tree_file));
	printf("data_file   = %s\n", data_file);
	printf("tree_file   = %s\n", tree_file);

	ifstream fp(data_file); 
	string line;
	int i=0;
	int randomNum = 100000;
	while (getline(fp,line)){ 
        string number;
        istringstream readstr(line); 
        
		getline(readstr,number,','); 
		table[i].key_ = atof(number.c_str()); 

		getline(readstr,number,','); 
		table[i].id_ = atoi(number.c_str());    
		if(table[i].id_ == 0) table[i].id_ = randomNum++;

		table_copy[i].key_ = table[i].key_;
		table_copy[i].id_ = table[i].id_;
        i++;
    }
	
	fp.close();

	timeval start_t, end_t;

	gettimeofday(&start_t,NULL);
		
	BTree* trees_ = new BTree();
	trees_->init(B_, tree_file);
	//对这个函数进行并行
	if(option == 0){
		if (trees_->bulkload1(n_pts_, table)) return 1;
	}
	else{
		if (trees_->bulkload(n_pts_, table)) return 1;
	}
	
	delete[] table; table = NULL;
	delete trees_; trees_ = NULL;

	gettimeofday(&end_t, NULL);

	float run_t1 = end_t.tv_sec - start_t.tv_sec + 
						(end_t.tv_usec - start_t.tv_usec) / 1000000.0f;
	printf("运行时间: %f  s\n", run_t1);
	
	check();
	return 0;
}

