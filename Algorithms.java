/**
 * @author axsun
 * This code is provided solely for CZ4031 assignment 2. This set of code shall NOT be redistributed.
 * You should provide implementation for the three algorithms declared in this class.  
 */

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
		Block[] memBuffers = new Block[M];
		ArrayList<ArrayList<Block>> rBuckets = new ArrayList<ArrayList<Block>>();
		ArrayList<ArrayList<Block>> sBuckets = new ArrayList<ArrayList<Block>>();
		
		// hash partition
		numIO = partition(relR, memBuffers, rBuckets, M, numIO);
		numIO = partition(relS, memBuffers, sBuckets, M, numIO);

		// join
		Block result = new Block();
		ArrayList<ArrayList<Block>> temp;
		boolean swap = false;
		// the size of the largest bucket is calculated during partition
		// for simplicity, we calculate it seperately here
		int bkSizeR = getLargestBucketSize(rBuckets);
		int bkSizeS = getLargestBucketSize(sBuckets);
		// System.out.println("bkSizeR: " + bkSizeR);
		// System.out.println("bkSizeS: " + bkSizeS);
		if(bkSizeS > M-1){
			if(bkSizeR > M-1){
				System.out.println("Error: One bucket cannot be fully loaded into memory buffer.");
				return 0;
			}
			temp = rBuckets;
			rBuckets = sBuckets;
			sBuckets = temp;
			swap = true;
		}

		// for each bucket of S
		for(int i = 0; i < M-1; i++){
			ArrayList<Block> sBucket = sBuckets.get(i);
			// load the entire bucket to memory
			initMemBuffers(memBuffers, M-1);
			int index = 0;
			for(Block block: sBucket){
				memBuffers[index] = block;
				numIO ++;
				index ++;
			}
			// load each block of R and perform in-memory join
			for(Block block: rBuckets.get(i)){
				memBuffers[M-1] = block;
				numIO ++;
				for(Tuple rTuple: memBuffers[M-1].tupleLst){
					for(int j = 0; j < index; j++){
						for(Tuple sTuple: memBuffers[j].tupleLst){
							if(rTuple.key == sTuple.key){
								JointTuple jt = null;
								if(swap){
									jt = new JointTuple(sTuple, rTuple);
								} else {
									jt = new JointTuple(rTuple, sTuple);
								}
								// if block full, write to disk
								// but we don't count the IO cost here
								while(!result.insertTuple(jt)){
									relRS.getRelationWriter().writeBlock(result);
									result = new Block();
								}
							}
						}
					}
				}
			}
		}
		// write the remaining block to disk
		if(result.tupleLst.size() > 0){
			relRS.getRelationWriter().writeBlock(result);
		}

		return numIO;
	}

	private int getTotalBlocks(ArrayList<ArrayList<Block>> diskBuckets){
		int num = 0;
		for(ArrayList<Block> bucket: diskBuckets){
			num += bucket.size();
		}
		return num;
	}

	private int getTotalTuples(ArrayList<ArrayList<Block>> diskBuckets){
		int num = 0;
		for(ArrayList<Block> bucket: diskBuckets){
			for(Block block: bucket){
				num += block.tupleLst.size();
			}
		}
		return num;
	}

	private void initMemBuffers(Block[] memBuffers, int size){
		for(int i = 0; i < size; i++)
			memBuffers[i] = new Block();
	}

	private void initDiskBuckets(ArrayList<ArrayList<Block>> diskBuckets, int size){
		for(int i = 0; i < size; i++){
			diskBuckets.add(new ArrayList<Block>());
		}
	}

	private int hash(int key, int size){
		int a = 8 * size / 23 + 5;
		return (a * key) % size;
	}

	private int partition(Relation r, Block[] memBuffers, ArrayList<ArrayList<Block>> diskBuckets, int M, int numIO){
		initMemBuffers(memBuffers, M-1);
		initDiskBuckets(diskBuckets, M-1);
		// should be careful, check whether neet to reset "iterator"
		Relation.RelationLoader rLoader = r.getRelationLoader();
		while(rLoader.hasNextBlock()){
			// load next block to the last memory buffer
			memBuffers[M-1] = rLoader.loadNextBlocks(1)[0];
			numIO ++;
			for(Tuple tuple: memBuffers[M-1].tupleLst){
				int id = hash(tuple.key, M-1);
				while(!memBuffers[id].insertTuple(tuple)){
					// if buffer full, write it to disk, empty buffer
					diskBuckets.get(id).add(memBuffers[id]);
					numIO ++;
					memBuffers[id] = new Block();
				}
			}
		}
		// for each buffer, if not empty, write to disk
		for(int i = 0; i < M-1; i++){
			if(memBuffers[i].tupleLst.size() > 0){
				diskBuckets.get(i).add(memBuffers[i]);
				numIO ++;
			}
		}
		// System.out.println("Total Number of tuples in buckets: " + getTotalTuples(diskBuckets));
		return numIO;
	}

	private int getLargestBucketSize(ArrayList<ArrayList<Block>> diskBuckets){
		int maxSize = 0;
		for(ArrayList<Block> bucket: diskBuckets){
			if(bucket.size() > maxSize){
				maxSize = bucket.size();
			}
		}
		return maxSize;
	}

	private int getTotalBlocksAllBuckets(Relation r, int M, int blockFactor){
		ArrayList<ArrayList<Tuple>> tuples = new ArrayList<ArrayList<Tuple>>();
		for(int i = 0; i < M - 1; i++){
			tuples.add(new ArrayList<Tuple>());
		}

		Block buffer;
		Relation.RelationLoader rLoader = r.getRelationLoader();
		rLoader.reset();
		while(rLoader.hasNextBlock()){
			// load next block to the last memory buffer
			buffer = rLoader.loadNextBlocks(1)[0];
			for(Tuple tuple: buffer.tupleLst){
				int id = hash(tuple.key, M-1);
				tuples.get(id).add(tuple);
			}
		}
		
		int totalBlocks = 0;
		for(ArrayList<Tuple> at: tuples){
			totalBlocks += (int) Math.ceil(((double) at.size())/blockFactor);
		}
		return totalBlocks;
	}

	private int getHashJoinTheoreticalIO(Relation relR, Relation relS, int M, int blockFactor) {
		return (getTotalBlocksAllBuckets(relR, M, blockFactor) + 
			getTotalBlocksAllBuckets(relS, M, blockFactor)) * 2 + 
			relR.getNumBlocks() + relS.getNumBlocks();
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
		Relation.RelationLoader rLoader=relR.getRelationLoader();		
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
		Relation.RelationWriter sWriter=relS.getRelationWriter();
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
		Algorithms algo = new Algorithms();

		/*Populate relations*/
		System.out.println("---------Populating two relations----------");
		Relation relR = new Relation("RelR");
		int numTuples = relR.populateRelationFromFile("RelR.txt");
		System.out.println("Relation RelR contains "+numTuples + " tuples.");
		Relation relS = new Relation("RelS");
		numTuples = relS.populateRelationFromFile("RelS.txt");
		System.out.println("Relation RelS contains " + numTuples + " tuples.");
		System.out.println("---------Finish populating relations----------\n\n");

		/*Test Hash Join*/
		Relation relRS = new Relation("RelRS");
		int numIO = algo.hashJoinRelations(relR, relS, relRS);
		System.out.println("---------Hash Join on relR and relS done----------");
		System.out.println("IO cost: " + numIO);
		numTuples = relRS.getNumTuples();
		System.out.println("Relation RelRS contains " + numTuples + " tuples.");

		int theoreticalIO = algo.getHashJoinTheoreticalIO(relR, relS, Setting.memorySize, Setting.blockFactor);

		if(theoreticalIO == numIO){
			System.out.println("numIO correct!");
		} else {
			System.out.println("numIO wrong!");
		}

		/*Print the relation */
		// System.out.println("---------Printing relation relRS----------");
		// relRS.printRelation(true, true);
		// select * FROM relr JOIN rels using (key) ORDER BY key;

		// Insert your test cases here!	
	}
	
	/**
	 * This main method provided for testing purpose
	 * @param arg
	 */
	public static void main(String[] arg){
		Algorithms.testCases();
	}
}
