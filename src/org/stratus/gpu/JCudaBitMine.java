/*
 * JCuda - Java bindings for NVIDIA CUDA driver and runtime API
 * http://www.jcuda.org
 *
 * Copyright 2011 Marco Hutter - http://www.jcuda.org
 */

import static jcuda.driver.JCudaDriver.*;

import java.io.*;

import jcuda.*;
import jcuda.driver.*;

/**
 * This is a sample class demonstrating how to use the JCuda driver
 * bindings to load and execute a CUDA vector addition kernel.
 * The sample reads a CUDA file, compiles it to a PTX file
 * using NVCC, loads the PTX file as a module and executes
 * the kernel function. <br />
 */
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/*
Steps to follow:
1. Calculate SHA256(SHA256([version + prev_block + merkle_root + timestamp + bits], nonce)). (Note: Here nonce should be in little endian hexstring.)
2. Compare result in step 1 with (expandedTarget)
3. If compare asserts that result of step 1 is less than that of step 2, then check the validity of the this nonce with [SuccessNonce in decimal]
4. Otherwise increment this nonce.
 */
//===============================================================================================================================================================
public class JCudaBitMine {

    /**
     * Entry point of this sample
     *
     * @param args Not used
     * @throws IOException If an IO error occurs
     */
//Total parts in each block: 4
    //Each part of the block is separated by a space.
    //[version + prev_block + merkle_root + timestamp + bits] [expanded target] [SuccessNonce in decimal] [resulting Hash]
    static String blockHeaders[] = {
        "0100000081cd02ab7e569e8bcd9317e2fe99f2de44d49ab2b8851ba4a308000000000000e320b6c2fffc8d750423db8b1eb942ae710e951ed797f7affc8892b0f1fc122bc7f5d74df2b9441a 00000000000044b9f20000000000000000000000000000000000000000000000 2504433986 00000000000000001e8d6829a8a21adc5d38d0a473b144b6765798e61f98bd1d",
        "010000001dbd981fe6985776b644b173a4d0385ddc1aa2a829688d1e0000000000000000b371c14921b20c2895ed76545c116e0ad70167c5c4952ca201f5d544a26efb53b4f6d74df2b9441a 00000000000044b9f20000000000000000000000000000000000000000000000 2165053959 0000000000001c0533ea776756cb6fdedbd952d3ab8bc71de3cd3f8a44cbaf85",
        "0100000085afcb448a3fcde31dc78babd352d9dbde6fcb566777ea33051c000000000000ca5b6b96fe65e1a7d50e7c3025a176472ba26d44512de86a6f3e39649330cd2f16f7d74df2b9441a 00000000000044b9f20000000000000000000000000000000000000000000000 2947380357 0000000000001112dff6e2a85b35d4f7ab7005b1b749282eeb1fdf094722601e",
        "010000001e60224709df1feb2e2849b7b10570abf7d4355ba8e2f6df121100000000000028cc65b7be2f8a1edc2af86ef369472443a1b70479cee205e8db5440cfbe943f57fad74df2b9441a 00000000000044b9f20000000000000000000000000000000000000000000000 1540236492 00000000000022177691274561ebb697c01447998ce579f57212470b6837cf98",
        "0100000098cf37680b471272f579e58c994714c097b6eb61452791761722000000000000b03c5b741b4ba5cc58b043cac824e441671ee3774e33d79618d5a80f36e9c85265fbd74df2b9441a 00000000000044b9f20000000000000000000000000000000000000000000000 2490931302 0000000000000d1b6f35712001533f259195136c25f14767813e215e49b0db4b",
        "010000004bdbb0495e213e816747f1256c139591253f53012071356f1b0d000000000000106c8b4ee453d3362e568fca0fd8f749e277618a908e703bb49fe353631d75e06afcd74df2b9441a 00000000000044b9f20000000000000000000000000000000000000000000000 3332866095 000000000000436aa3bacdf7fca3584729a777d4f4db6b8c987260bbe6366b31",
        "01000000316b36e6bb6072988c6bdbf4d477a7294758a3fcf7cdbaa36a430000000000009d77d14e74506b41a025d6a208665c7f7c17423d45e1590af1efaf12e87ba0fc26fdd74df2b9441a 00000000000044b9f20000000000000000000000000000000000000000000000 1737841105 00000000000043117e5e9fc97bbcc5bb84e8d5e57858905c57d83ee57af23956",
        "010000005639f27ae53ed8575c905878e5d5e884bbc5bc7bc99f5e7e1143000000000000dbc06f4c083bd0e327d025329006e40a9f976bb3bfc70a41bb38fb75ca2d151f2ffdd74df2b9441a 00000000000044b9f20000000000000000000000000000000000000000000000 2308925096 00000000000025639135c036c61ae65075f34a77853924fd83a76114a9448d04",
        "01000000048d44a91461a783fd243985774af37550e61ac636c035916325000000000000d278032f90166f89ae0fb6a3bb331b6bf7395aee5763be80eaf32fb5e8754c2f0d00d84df2b9441a 00000000000044b9f20000000000000000000000000000000000000000000000 2835572962 000000000000366cadd9d59c95a47cf5a073bb728dadcd2eb6d8035ebcb0f0ba",
        "01000000baf0b0bc5e03d8b62ecdad8d72bb73a0f57ca4959cd5d9ad6c3600000000000076e0569c5dd0392739003d2e4480a0a3c1f37f3cd06488e5eb439c63759649575a00d84df2b9441a 00000000000044b9f20000000000000000000000000000000000000000000000 330002446 0000000000001df899cb037d1a98e619ef374d62f5b734e05eb6f778c2782609"
    };

    public static String bigToLittle(String str) {
        String result = "";

        if (str.length() % 2 != 0) {
            str = "0" + str;
        }

        for (int i = 0; i < str.length(); i = i + 2) {
            result = str.substring(i, i + 2) + result;
        }

        return result;
    }

    public static void main(String args[]) throws IOException {



        // Enable exceptions and omit all subsequent error checks
        JCudaDriver.setExceptionsEnabled(true);

        // Create the PTX file by calling the NVCC
        String ptxFileName = preparePtxFile("JCudaBitMine.cu");

        // Initialize the driver and create a context for the first device.
        cuInit(0);
        CUdevice device = new CUdevice();
        cuDeviceGet(device, 0);
        CUcontext context = new CUcontext();
        cuCtxCreate(context, 0, device);

        // Load the ptx file.
        CUmodule module = new CUmodule();
        cuModuleLoad(module, ptxFileName);

        // Obtain a function pointer to the "add" function.
        CUfunction function = new CUfunction();
        cuModuleGetFunction(function, module, "inversehash");
        //Get the startNounce From header.
        String blockHeader = blockHeaders[0]; //using first block for this example
        String inputSegments[] = blockHeader.split(" "); //creating tokens
        
        
        long successNonce = Long.parseLong(inputSegments[2]); //third token: real nonce at success
        String resultHash = inputSegments[3]; //fourth token: resulting hash at success

        int nonceStart = 0;
        int nonceLimit = 500000;
        long nonce = 0;
        //Create block Size of 10000 input.
        //int nonceBreakPoint = 10000;

        String shaInput = inputSegments[0]; //first token: shaInput excluding nonce
        byte byteshainput[] = hexStringToByteArray(shaInput);
        int shaInputLength =  shaInput.length();

        CUdeviceptr InputSha = new CUdeviceptr();
        cuMemAlloc(InputSha, shaInputLength * Sizeof.BYTE);
        cuMemcpyHtoD(InputSha, Pointer.to(byteshainput), shaInputLength * Sizeof.BYTE);


        String expandedTarget = inputSegments[1]; //second token: target
        byte bytetarget[] = hexStringToByteArray(expandedTarget);
        int expandedTargetLength =  expandedTarget.length();

        CUdeviceptr InputTarget = new CUdeviceptr();
        cuMemAlloc(InputTarget, expandedTargetLength * Sizeof.BYTE);
        cuMemcpyHtoD(InputTarget, Pointer.to(bytetarget), expandedTargetLength * Sizeof.BYTE);

        long inputNonce[] = new long[nonceLimit];
        while (nonceStart <= nonceLimit) {
            //check for boundary case:In case tmpNonceLimit becomes greator than nonceLimit.
            //convert nonce in decimal to little-endian hex string form
            inputNonce[nonceStart] = nonce;
            nonceStart++;
            nonce++;
        }

        CUdeviceptr InputNonce = new CUdeviceptr();
        cuMemAlloc(InputNonce, nonceLimit * Sizeof.INT);
        cuMemcpyHtoD(InputNonce, Pointer.to(inputNonce), nonceLimit * Sizeof.LONG);

        // Allocate device output memory
        long hostOutput[] = new long[1];
        
        CUdeviceptr deviceOutput = new CUdeviceptr();
        cuMemAlloc(deviceOutput, Sizeof.LONG);


        // Set up the kernel parameters: A pointer to an array
        // of pointers which point to the actual values.
        Pointer kernelParameters = Pointer.to(
                Pointer.to(new int[]{nonceLimit}),
                Pointer.to(InputSha),
                Pointer.to(InputNonce),
                Pointer.to(InputTarget),
                Pointer.to(deviceOutput));

        // Call the kernel function.
        // Call the kernel function.
        int blockSizeX = 256;
        int gridSizeX = (int)Math.ceil((double)nonceLimit / blockSizeX);
        cuLaunchKernel(function,
                gridSizeX, 1, 1, // Grid dimension
                blockSizeX, 1, 1, // Block dimension
                0, null, // Shared memory size and stream
                kernelParameters, null // Kernel- and extra parameters
                );
        cuCtxSynchronize();

        // Copy each row back from the device to the host
        cuMemcpyDtoH(Pointer.to(hostOutput), deviceOutput,Sizeof.LONG);
        
        // Print the results
        System.out.println(hostOutput[0]);
        
        cuMemFree(InputSha);
        cuMemFree(InputNonce);
        cuMemFree(InputTarget);
        cuMemFree(deviceOutput);
    }

    /**
     * The extension of the given file name is replaced with "ptx".
     * If the file with the resulting name does not exist, it is
     * compiled from the given file using NVCC. The name of the
     * PTX file is returned.
     *
     * @param cuFileName The name of the .CU file
     * @return The name of the PTX file
     * @throws IOException If an I/O error occurs
     */
    private static String preparePtxFile(String cuFileName) throws IOException {
        int endIndex = cuFileName.lastIndexOf('.');
        if (endIndex == -1) {
            endIndex = cuFileName.length() - 1;
        }
        String ptxFileName = cuFileName.substring(0, endIndex + 1) + "ptx";
        File ptxFile = new File(ptxFileName);
        if (ptxFile.exists()) {
            return ptxFileName;
        }

        File cuFile = new File(cuFileName);
        if (!cuFile.exists()) {
            throw new IOException("Input file not found: " + cuFileName);
        }
        String modelString = "-m" + System.getProperty("sun.arch.data.model");
        String command =
                "nvcc " + modelString + " -ptx "
                + cuFile.getPath() + " -o " + ptxFileName;

        System.out.println("Executing\n" + command);
        Process process = Runtime.getRuntime().exec(command);

        String errorMessage =
                new String(toByteArray(process.getErrorStream()));
        String outputMessage =
                new String(toByteArray(process.getInputStream()));
        int exitValue = 0;
        try {
            exitValue = process.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(
                    "Interrupted while waiting for nvcc output", e);
        }

        if (exitValue != 0) {
            System.out.println("nvcc process exitValue " + exitValue);
            System.out.println("errorMessage:\n" + errorMessage);
            System.out.println("outputMessage:\n" + outputMessage);
            throw new IOException(
                    "Could not create .ptx file: " + errorMessage);
        }

        System.out.println("Finished creating PTX file");
        return ptxFileName;
    }

    /**
     * Fully reads the given InputStream and returns it as a byte array
     *
     * @param inputStream The input stream to read
     * @return The byte array containing the data from the input stream
     * @throws IOException If an I/O error occurs
     */
    private static byte[] toByteArray(InputStream inputStream)
            throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte buffer[] = new byte[8192];
        while (true) {
            int read = inputStream.read(buffer);
            if (read == -1) {
                break;
            }
            baos.write(buffer, 0, read);
        }
        return baos.toByteArray();
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    /*
    public void test(){
    public class JCudaCharPointersTest
    {
    public static void main(String args[]) throws IOException
    {
    CUfunction function = defaultInit();

    // Create the host input memory: The bytes of a String
    String inputString = "Hello pointers";
    byte hostInput[] = inputString.getBytes();
    int size = hostInput.length;

    // Allocate and fill the device input memory
    CUdeviceptr deviceInput = new CUdeviceptr();
    cuMemAlloc(deviceInput, size * Sizeof.BYTE);
    cuMemcpyHtoD(deviceInput, Pointer.to(hostInput),
    size * Sizeof.BYTE);

    // Allocate the host output memory: 'numThreads' arrays
    // of bytes, each receiving a copy of the input
    int numThreads = 8;
    byte hostOutput[][] = new byte[numThreads][size];

    // Allocate arrays on the device, one for each row. The pointers
    // to these array are stored in host memory.
    CUdeviceptr hostDevicePointers[] = new CUdeviceptr[numThreads];
    for(int i = 0; i < numThreads; i++)
    {
    hostDevicePointers[i] = new CUdeviceptr();
    cuMemAlloc(hostDevicePointers[i], size * Sizeof.BYTE);
    }

    // Allocate device memory for the array pointers, and copy
    // the array pointers from the host to the device.
    CUdeviceptr deviceOutput = new CUdeviceptr();
    cuMemAlloc(deviceOutput, numThreads * Sizeof.POINTER);
    cuMemcpyHtoD(deviceOutput, Pointer.to(hostDevicePointers),
    numThreads * Sizeof.POINTER);

    // Set up the kernel parameters: A pointer to an array
    // of pointers which point to the actual values.
    Pointer kernelParams = Pointer.to(
    Pointer.to(new int[]{size}),
    Pointer.to(deviceInput),
    Pointer.to(deviceOutput)
    );

    // Call the kernel function.
    cuLaunchKernel(function,
    1, 1, 1, // Grid dimension
    numThreads, 1, 1, // Block dimension
    0, null, // Shared memory size and stream
    kernelParams, null // Kernel- and extra parameters
    );
    cuCtxSynchronize();

    // Copy each row back from the device to the host
    for(int i = 0; i < numThreads; i++)
    {
    cuMemcpyDtoH(Pointer.to(hostOutput[i]), hostDevicePointers[i],
    size * Sizeof.BYTE);
    }

    // Print the results
    boolean passed = true;
    for(int i = 0; i < numThreads; i++)
    {
    String s = new String(hostOutput[i]);
    if (!s.equals(inputString))
    {
    passed = false;
    }
    System.out.println(s);
    }
    System.out.println("Test "+(passed?"PASSED":"FAILED"));

    // Clean up.
    for(int i = 0; i < numThreads; i++)
    {
    cuMemFree(hostDevicePointers[i]);
    }
    cuMemFree(deviceInput);
    cuMemFree(deviceOutput);
    }

    private static CUfunction defaultInit() throws IOException
    {
    // Enable exceptions and omit all subsequent error checks
    JCudaDriver.setExceptionsEnabled(true);

    // Create the PTX file by calling the NVCC
    String ptxFileName = preparePtxFile("JCudaCharPointersTestKernel.cu");

    // Initialize the driver and create a context for the first device.
    cuInit(0);
    CUdevice device = new CUdevice();
    cuDeviceGet(device, 0);
    CUcontext context = new CUcontext();
    cuCtxCreate(context, 0, device);

    // Load the ptx file.
    CUmodule module = new CUmodule();
    cuModuleLoad(module, ptxFileName);

    // Obtain a function pointer to the "sampleKernel" function.
    CUfunction function = new CUfunction();
    cuModuleGetFunction(function, module, "sampleKernel");

    return function;
    }

    }*/
}
