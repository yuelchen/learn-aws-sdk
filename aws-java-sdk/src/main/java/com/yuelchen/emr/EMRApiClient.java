package com.yuelchen.emr;

import java.util.List;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.InternalServerErrorException;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

/**
 * Amazon EMR API client handler for performing EMR operations. 
 * 
 * @author 	yuelchen
 * @version	1.0.0
 * @since 	2019-09-01
 */
public class EMRApiClient {
	
	/**
	 * The default Amazon EMR Client for making API requests. 
	 */
	private static AmazonElasticMapReduce amazonEMRClient = 
			AmazonElasticMapReduceClientBuilder.defaultClient();

    //====================================================================================================
    
    /** 
     * Private constructor.
     */
	private EMRApiClient() {}
    
    //====================================================================================================
	
	/**
	 * Returns a list of steps identifiers for added steps. 
	 * 
	 * @param clusterId							the EMR cluster Id.
	 * @param stepName							the name of the step.
	 * @param jarLocation						the custom jar to be executed as step. 
	 * 
	 * @return									a list of steps identifiers.
	 * 
	 * @throws InternalServerErrorException		thrown when an error occurs while processing the request
	 * 											and could not be completed. 
	 */
	public static List<String> submitEMRStep(String clusterId, String stepName, String jarLocation) 
			throws InternalServerErrorException {
		
		HadoopJarStepConfig hadoopJarStepConfig = new HadoopJarStepConfig()
				.withJar(jarLocation);
		StepConfig stepConfig = new StepConfig(stepName, hadoopJarStepConfig);
		AddJobFlowStepsRequest addJobFlowStepsRequest = new AddJobFlowStepsRequest()
				.withJobFlowId(clusterId)
				.withSteps(stepConfig);
		
		AddJobFlowStepsResult addJobFlowStepsResult = 
				amazonEMRClient.addJobFlowSteps(addJobFlowStepsRequest);
		return addJobFlowStepsResult.getStepIds();
	}
}