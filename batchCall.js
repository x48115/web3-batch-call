const Web3 = require("web3");
const _ = require("lodash");
const AbiStorage = require("./utils/abiStorage");

class BatchCall {
  constructor(config) {
    const {
      web3,
      provider,
      groupByNamespace,
      logging,
      simplifyResponse,
      store,
    } = config;

    if (typeof web3 === "undefined" && typeof provider === "undefined") {
      throw new Error(
        "You need to either provide a web3 instance or a provider string ser!"
      );
    }

    if (web3) {
      this.web3 = web3;
    } else {
      this.web3 = new Web3(provider);
    }

    const { etherscan = {} } = config;
    const { apiKey: etherscanApiKey = null, delayTime = 300 } = etherscan;

    this.etherscanApiKey = etherscanApiKey;
    this.etherscanDelayTime = delayTime;
    this.abiHashByAddress = {};
    this.abiByHash = {};
    this.groupByNamespace = groupByNamespace;
    this.logging = logging;
    this.simplifyResponse = simplifyResponse;
    this.readContracts = {};
    this.store = new AbiStorage({
      store,
      etherscanApiKey,
      logging,
    });
  }

  async execute(contractsBatch, callOptions) {
    // Constants
    const startTime = Date.now();
    const blockHeight = _.get(callOptions, "blockHeight", 1);
    const blockResolution = _.get(callOptions, "blockResolution", 1);
    const { web3, store, readContracts, groupByNamespace, logging } = this;
    let numberOfMethods = 0;
    const currentBlockNumber = await web3.eth.getBlockNumber();

    // Build list of blocks to read ("blocks")
    let blocks = [];
    let blockNumberIterator = 0;
    while (blockNumberIterator < blockHeight) {
      const blockNumber = currentBlockNumber - blockNumberIterator;
      blocks.push(blockNumber);
      blockNumberIterator += blockResolution;
    }
    blocks = _.reverse(blocks);

    // First level. Add each contractConfig from the batch request
    const addContractToBatch = async (batch, contractConfig) => {
      const {
        addresses,
        contracts,
        namespace = "default",
        readMethods = [],
        allReadMethods,
      } = contractConfig;
      const objectToIterateOver = addresses || contracts;
      const addressPromises = await objectToIterateOver.map(
        addAddressToBatch.bind(
          null,
          batch,
          readMethods,
          allReadMethods,
          namespace
        )
      );
      return await Promise.all(addressPromises);
    };

    // Read each address of a contractConfig
    const addAddressToBatch = async (
      batch,
      readMethods,
      allReadMethods,
      namespace,
      item
    ) => {
      const itemIsContract = item.options;
      let address;
      let abi;

      // Fetch ABI from contract or get/build it from store cache
      if (itemIsContract) {
        address = item.options.address;
        abi = item.options.jsonInterface;
      } else {
        address = item;
        abi = store.getAbiFromCache(address);
      }

      // Initialize the current contract
      const contract = new web3.eth.Contract(abi, address);

      // Get all readable methods from the contract ABI
      let allMethods = _.clone(readMethods);
      if (allReadMethods) {
        const formatField = (name) => ({ name });
        const allFields = store.getReadableAbiFields(address).map(formatField);
        allMethods.push(...allFields);
      }

      // Don't read constants more than once
      const filterOutConstants = (method) => {
        const readContractAtLeastOnce = readContracts[address];
        const { constant } = method;
        if (constant && readContractAtLeastOnce) {
          return false;
        }
        return true;
      };
      allMethods = _.filter(allMethods, filterOutConstants);

      // For every method go and asynchronously add the method to the batch
      const methodsPromises = await allMethods.map(
        addMethodToBatch.bind(null, batch, contract, abi, address)
      );
      const methodsState = await Promise.all(methodsPromises);
      readContracts[address] = true;
      return Promise.resolve({
        address,
        namespace,
        state: methodsState,
      });
    };

    // For every block in "blocks" range and for every contract address read all methods
    const addBlockToBatch = (
      batch,
      args,
      abiMethod,
      address,
      methodCall,
      name,
      blockNumber
    ) =>
      new Promise((blockResolve) => {
        try {
          const returnResponse = (blockNumber, err, data) => {
            if (err) {
              console.log(
                `[BatchCall] ${address}: method call failed: ${name}`
              );
            }
            blockResolve({
              value: data,
              blockNumber,
            });
          };
          let req;
          req = methodCall.request(
            blockNumber,
            returnResponse.bind(null, blockNumber)
          );
          numberOfMethods += 1;

          // Add the method call to our batch request
          batch.add(req);
        } catch (err) {
          console.log("method err");
          blockResolve();
        }
      });

    // Build method response
    const addMethodToBatch = async (batch, contract, abi, address, method) => {
      const { name, args } = method;
      const abiMethod = _.find(abi, { name });

      let methodCall;
      const methodExists = _.get(contract.methods, name);
      if (!methodExists) {
        return Promise.resolve();
      }
      const nbrAbiArgsForMethod = _.size(abiMethod.inputs);
      const newArgs = _.take(args, nbrAbiArgsForMethod);

      // Build method call
      if (newArgs) {
        methodCall = contract.methods[name](...newArgs).call;
      } else {
        methodCall = contract.methods[name]().call;
      }

      // Get method input data
      const input = args && web3.eth.abi.encodeFunctionCall(abiMethod, newArgs);

      // Read method for every block in "blocks" range
      const blocksPromises = await blocks.map(
        addBlockToBatch.bind(
          null,
          batch,
          args,
          abiMethod,
          address,
          methodCall,
          name
        )
      );
      const blocksState = await Promise.all(blocksPromises);
      readContracts[address] = true;
      return Promise.resolve({
        address,
        input,
        method: method.name,
        values: blocksState,
        args,
      });
    };

    // Build contracts state response. Don't allow duplicate addresses, merge data when necessary
    const formatContractsState = (acc, contractConfig) => {
      const addMethodResults = (address, namespace, result) => {
        if (!result) {
          return acc;
        }
        const { method, values, input, args } = result;
        const addressResult = _.find(acc, { address }) || {};
        const foundAddressResult = _.size(addressResult);
        const methodArgs = _.get(addressResult, method, []);
        const existingMethodInput = _.find(methodArgs, { input });
        const methodArg = {
          values,
          input,
          args,
        };
        if (!input) {
          delete methodArg.input;
        }
        if (!args) {
          delete methodArg.args;
        }
        if (!existingMethodInput && foundAddressResult) {
          methodArgs.push(methodArg);
          addressResult[method] = methodArgs;
        }
        if (!input && foundAddressResult) {
          addressResult[method] = [methodArg];
        }
        if (!foundAddressResult) {
          const newAddressResult = {
            address,
            namespace,
          };
          newAddressResult[method] = [methodArg];
          acc.push(newAddressResult);
        }
      };
      const addAddressCalls = (addressCall) => {
        const { address, state, namespace } = addressCall;
        _.each(state, addMethodResults.bind(null, address, namespace));
      };
      _.each(contractConfig, addAddressCalls);
      return acc;
    };

    // Build or cache ABIs
    const addAbis = async (contractBatch) => {
      const { abi, addresses } = contractBatch;
      for (const address of addresses) {
        await store.addAbiToCache(address, abi);
      }
    };
    for (const contractBatch of contractsBatch) {
      const { contracts } = contractBatch;
      if (!contracts) {
        await addAbis(contractBatch);
      }
    }

    // Create batch
    const batch = new web3.BatchRequest();

    // Initial entry point. Start building batch
    const contractsPromises = contractsBatch.map(
      addContractToBatch.bind(null, batch)
    );

    // Execute batch call
    let contractsState;
    batch.execute();
    const contractsPromiseResult = await Promise.all(contractsPromises);
    contractsState = _.reduce(contractsPromiseResult, formatContractsState, []);
    let contractsToReturn = contractsState;

    /**
     * Post-processing
     */
    const flattenArgs = (contract) => {
      const flattenArg = (values, key) => {
        const valuesIsArr = _.isArray(values);
        const flattenVal = (value, idx) => {
          const innerVals = value.values;
          const onlyOneVal = _.size(innerVals) === 1;
          if (onlyOneVal) {
            // Replace "values" with "value" and flatten result when possible. If more than one blocks are read this will not happen
            contract[key][idx].value = innerVals[0].value;
            delete contract[key][idx].values;
          }

          // If only one arg is read and simplifyResponse is true flatten response even further
          const onlyOneArg = _.size(values) === 1;
          if (onlyOneArg && this.simplifyResponse) {
            const { value } = values[0];
            if (value) {
              contract[key] = value;
            } else {
              contract[key] = _.get(values, `[0].values`);
            }
          }
        };

        // Only flatten arrays
        if (valuesIsArr) {
          _.each(values, flattenVal);
        }
      };
      _.each(contract, flattenArg);
      return contract;
    };
    contractsToReturn = _.map(contractsToReturn, flattenArgs);

    // TODO: Test if this still works???
    if (groupByNamespace) {
      const contractsStateByNamespace = _.groupBy(contractsState, "namespace");
      const removeNamespaceKey = (acc, contracts, key) => {
        const omitNamespace = (contract) => _.omit(contract, "namespace");
        acc[key] = _.map(contracts, omitNamespace);
        return acc;
      };

      const contractsStateByNamespaceReduced = _.reduce(
        contractsStateByNamespace,
        removeNamespaceKey,
        {}
      );
      contractsToReturn = contractsStateByNamespaceReduced;
    }

    // Add logging if logging flag is enabled
    if (logging) {
      const endTime = Date.now();
      const executionTime = endTime - startTime;
      console.log(
        `[BatchCall] methods: ${numberOfMethods}, execution time: ${executionTime} ms`
      );
    }
    return contractsToReturn;
  }
}

module.exports = BatchCall;
