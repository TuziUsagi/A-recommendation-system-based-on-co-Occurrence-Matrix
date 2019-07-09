package recommenderSystem;

public class Recommender_driver
{
    public static void main(String[] args) throws Exception
    {
        //data processing through 5 mapreduce jobs: userRatings -> coOccurrenceMatrix -> normalization -> matrixMultiplication -> sumMovieWeights
        //args[0]: input file directory for userRatings
        //args[1]: output directory for userRating, same as input directory for coOccurrenceMatrix
        //args[2]: output directory for coOccurrenceMatrix, same as the input directory for normalization
        //args[3]: output directory for normalization, same as the input directory for matrixMultiplication
        //args[4]: output directory for matrixMultiplication, same as the input directory for sumMovieWeights
        //args[5]: output directory for sumMovieWeights and the final result

        String ratingRawData = args[0];
        String userRatingsOut = args[1];
        String coOccurrenceMatrixOut = args[2];
        String normalizationOut = args[3];
        String matrixMultiplicationOut = args[4];
        String sumMovieWeightsOut = args[5];

        String[] MR1 = {ratingRawData,userRatingsOut};
        UserRatings userRatings = new UserRatings();
        userRatings.main(MR1);

        String[] MR2 = {userRatingsOut,coOccurrenceMatrixOut};
        CoOccurrenceMatrix coOccurrenceMatrix = new CoOccurrenceMatrix();
        coOccurrenceMatrix.main(MR2);

        String[] MR3 = {coOccurrenceMatrixOut,normalizationOut};
        Normalization normalization = new Normalization();
        normalization.main(MR3);

        String[] MR4 = {normalizationOut,ratingRawData, matrixMultiplicationOut};
        MatrixMultiplication matrixMultiplication = new MatrixMultiplication();
        matrixMultiplication.main(MR4);

        String[] MR5 = {matrixMultiplicationOut,sumMovieWeightsOut};
        SumMovieWeights sumMovieWeights = new SumMovieWeights();
        sumMovieWeights.main(MR5);

    }

}
