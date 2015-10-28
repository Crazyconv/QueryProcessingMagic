/**
 * @author axsun
 * This code is provided solely for CZ4031 assignment 2. This set of code shall NOT be redistributed.
 * You should provide implementation for the three algorithms declared in this class.  
 */

package project2;

import project2.Relation.RelationLoader;
import project2.Relation.RelationWriter;
import java.util.ArrayList;

public class Algorithms {
	
	/**
	 * Sort the relation using Setting.memorySize buffers of memory 
	 * @param rel is the relation to be sorted. 
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 */
	public int mergeSortRelation(Relation rel){
		int numIO=0;
		
		//Insert your code here!
		
		return numIO;
	}
	
	/**
	 * Join relations relR and relS using Setting.memorySize buffers of memory to produce the result relation relRS
	 * @param relR is one of the relation in the join
	 * @param relS is the other relation in the join
	 * @param relRS is the result relation of the join
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 */
	public int hashJoinRelations(Relation relR, Relation relS, Relation relRS){
		int numIO=0;
		int M = Setting.memorySize;
		Block[] memBuffers = new Block[M]();
		ArrayList<ArrayList<Block>> rBuckets = new ArrayList<ArrayList<Block>>();
		ArrayList<ArrayList<Block>> sBuckets = new ArrayList<ArrayList<Block>>();
		
		// hash partition
		numIO = partition(relR, memBuffers, rBuckets, M, numIO);
		memBuffers = new Block[M]();
		numIO = partition(relS, memBuffers, sBuckets, M, numIO);

		// join
		Block result = new Block();
		ArrayList<ArrayList<Block>> temp;
		boolean swap = false;
		// the size of the largest bucket is calculated during partition
		// for simplicity, we calculate it seperately here
		int bkSizeR = getLargestBucketSize(rBuckets);
		int bkSizeS = getLargestBucketSize(sBuckets);
		if(bkSizeS > M-1){
			if(bkSizeR > M-1){
				System.out.println("Error: One bucket cannot be fully loaded into memory buffer.")
				return 0;
			}
			sBuckets = temp;
			temp = rBuckets;
			rBuckets = sBuckets;
			swap = true;
		}

		// for each bucket of S
		for(int i = 0; i < M-1; i++){
			ArrayList<Block> sBucket = sBuckets[i];
			// load the entire bucket to memory
			memBuffers = new Block[M]();
			int index = 0;
			for(Block block: sBucket){
				memBuffers[index] = block;
				numIO ++;
				index ++;
			}
			// load each block of R and perform in-memory join
			for(Block block: rBuckets[i]){
				memBuffers[M-1] = block;
				numIO ++;
				for(Tuple rTuple: memBuffers[M-1].tupleLst){
					for(int j = 0; j < index; j++){
						for(Tuple sTuple: memBuffers[j].tupleLst){
							if(rTuple.key == sTuple.key){
								if(swap){
									JointTuple jt = new JointTuple(sTuple, rTuple);
								} else {
									JointTuple jt = new JointTuple(rTuple, sTuple);
								}
								// if block full, write to disk
								while(!result.insertTuple(jt)){
									relRS.getRelationWriter.writeBlock(result);
									numIO ++;
									result = new Block();
								}
							}
						}
					}
				}
			}

		}

		return numIO;
	}

	private int hash(int key, int size){
		int a = 8 * size / 23 + 5;
		return (a * key) % size;
	}

	private int partition(Relation r, Block[] memBuffers, ArrayList<ArrayList<Block>> disk_buckets, int M, int numIO){
		// should be careful, check whether neet to reset "iterator"
		RelationLoader rLoader = r.getRelationLoader();
		while(rLoader.hasNextBlock()){
			// load next block to the last memory buffer
			memBuffers[M-1] = rLoader.loadNextBlocks(1)[0];
			numIO ++;
			for(Tuple tuple: memBuffers[M-1].tupleLst){
				int id = hash(tuple.key, M-1);
				while(!memBuffers[id].insertTuple(tuple)){
					// if buffer full, write it to disk, empty buffer
					disk_buckets.get(id).add(memBuffers[id]);
					numIO ++;
					memBuffers[id] = new Block();
				}
			}
		}
		// for each buffer, if not empty, write to disk
		for(int i = 0; i < M-1; i++){
			if(memBuffers[i].tupleLst.length > 0){
				disk_buckets.get(id).add(memBuffers[i]);
				numIO ++;
			}
		}
		return numIO;
	}
	
	/**
	 * Join relations relR and relS using Setting.memorySize buffers of memory to produce the result relation relRS
	 * @param relR is one of the relation in the join
	 * @param relS is the other relation in the join
	 * @param relRS is the result relation of the join
	 * @return the number of IO cost (in terms of reading and writing blocks)
	 */
	
	public int refinedSortMergeJoinRelations(Relation relR, Relation relS, Relation relRS){
		int numIO=0;
		
		//Insert your code here!
		
		return numIO;
	}

	
	
	/**
	 * Example usage of classes. 
	 */
	public static void examples(){

		/*Populate relations*/
		System.out.println("---------Populating two relations----------");
		Relation relR=new Relation("RelR");
		int numTuples=relR.populateRelationFromFile("RelR.txt");
		System.out.println("Relation RelR contains "+numTuples+" tuples.");
		Relation relS=new Relation("RelS");
		numTuples=relS.populateRelationFromFile("RelS.txt");
		System.out.println("Relation RelS contains "+numTuples+" tuples.");
		System.out.println("---------Finish populating relations----------\n\n");
			
		/*Print the relation */
		System.out.println("---------Printing relations----------");
		relR.printRelation(true, true);
		relS.printRelation(true, false);
		System.out.println("---------Finish printing relations----------\n\n");
		
		
		/*Example use of RelationLoader*/
		System.out.println("---------Loading relation RelR using RelationLoader----------");
		RelationLoader rLoader=relR.getRelationLoader();		
		while(rLoader.hasNextBlock()){
			System.out.println("--->Load at most 7 blocks each time into memory...");
			Block[] blocks=rLoader.loadNextBlocks(7);
			//print out loaded blocks 
			for(Block b:blocks){
				if(b!=null) b.print(false);
			}
		}
		System.out.println("---------Finish loading relation RelR----------\n\n");
				
		
		/*Example use of RelationWriter*/
		System.out.println("---------Writing to relation RelS----------");
		RelationWriter sWriter=relS.getRelationWriter();
		rLoader.reset();
		if(rLoader.hasNextBlock()){
			System.out.println("Writing the first 7 blocks from RelR to RelS");
			System.out.println("--------Before writing-------");
			relR.printRelation(false, false);
			relS.printRelation(false, false);
			
			Block[] blocks=rLoader.loadNextBlocks(7);
			for(Block b:blocks){
				if(b!=null) sWriter.writeBlock(b);
			}
			System.out.println("--------After writing-------");
			relR.printRelation(false, false);
			relS.printRelation(false, false);
		}

	}
	
	/**
	 * Testing cases. 
	 */
	public static void testCases(){
	
		// Insert your test cases here!
	
	}
	
	/**
	 * This main method provided for testing purpose
	 * @param arg
	 */
	public static void main(String[] arg){
		Algorithms.examples();
	}
}
