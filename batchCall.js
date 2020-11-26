const Web3 = require("web3");
const _ = require("lodash");
const md5 = require("md5");
const delay = require("delay");
const fetch = require("cross-fetch");

class BatchCall {
  constructor(config) {
    const {
      web3,
      provider,
      groupByNamespace,
      clearMemoryAfterExecution,
      logging,
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
    const { apiKey = null, delayTime = 300 } = etherscan;

    this.etherscanApiKey = apiKey;
    this.etherscanDelayTime = delayTime;
    this.abiHashByAddress = {};
    this.abiByHash = {};
    this.groupByNamespace = groupByNamespace;
    this.clearMemoryAfterExecution = clearMemoryAfterExecution;
    this.logging = logging;
  }

  async execute(contractsBatch, blockNumber) {
    const startTime = Date.now();
    let numberOfMethods = 0;
    const { web3 } = this;
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
      if (itemIsContract) {
        address = item.options.address;
        abi = item.options.jsonInterface;
      } else {
        address = item;
        abi = this.getAbiFromCache(address);
      }

      const contract = new web3.eth.Contract(abi, address);

      let allMethods = _.clone(readMethods);
      if (allReadMethods) {
        const formatField = (name) => ({ name });
        const allFields = this.getReadableAbiFields(address).map(formatField);
        allMethods.push(...allFields);
      }

      const methodsPromises = await allMethods.map(
        addMethodToBatch.bind(null, batch, contract, abi, address)
      );
      const methodsState = await Promise.all(methodsPromises);
      return Promise.resolve({
        address,
        namespace,
        state: methodsState,
      });
    };

    const addMethodToBatch = (batch, contract, abi, address, method) =>
      new Promise((methodResolve, methodReject) => {
        const { name, args } = method;
        let methodCall;
        const methodExists = _.get(contract.methods, name);
        if (!methodExists) {
          return methodResolve();
        }
        if (args) {
          methodCall = contract.methods[name](...args).call;
        } else {
          methodCall = contract.methods[name]().call;
        }
        numberOfMethods += 1;
        const returnResponse = (err, data) => {
          if (err) {
            methodReject(err);
          } else {
            const abiMethod = _.find(abi, { name });
            const input =
              args && web3.eth.abi.encodeFunctionCall(abiMethod, args);
            methodResolve({
              method: method.name,
              value: data,
              input,
              args,
            });
          }
        };
        let req;
        if (blockNumber) {
          req = methodCall.request(blockNumber, returnResponse);
        } else {
          req = methodCall.request(returnResponse);
        }
        batch.add(req);
      });

    const formatContractsState = (acc, contractConfig) => {
      const addMethodResults = (address, namespace, result) => {
        if (!result) {
          return acc;
        }
        const { method, value, input, args } = result;
        const addressResult = _.find(acc, { address }) || {};
        const foundAddressResult = _.size(addressResult);
        const methodArgs = _.get(addressResult, method, []);
        const existingMethodInput = _.find(methodArgs, { input });
        const methodArg = {
          value,
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

    const addAbis = async (contractBatch) => {
      const { abi, addresses } = contractBatch;
      for (const address of addresses) {
        await this.addAbiToCache(address, abi);
      }
    };
    for (const contractBatch of contractsBatch) {
      const { contracts } = contractBatch;
      if (!contracts) {
        await addAbis(contractBatch);
      }
    }

    const batch = new web3.BatchRequest();
    const contractsPromises = contractsBatch.map(
      addContractToBatch.bind(null, batch)
    );

    let contractsState;
    batch.execute();
    try {
      const contractsPromiseResult = await Promise.all(contractsPromises);
      contractsState = _.reduce(
        contractsPromiseResult,
        formatContractsState,
        []
      );
    } catch (err) {
      return { error: err.message };
    }

    let contractsToReturn = contractsState;
    if (this.groupByNamespace) {
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
    if (this.clearMemoryAfterExecution) {
      this.abiHashByAddress = {};
      this.abiByHash = {};
    }
    if (this.logging) {
      const endTime = Date.now();
      const executionTime = endTime - startTime;
      console.log(
        `[BatchCall] methods: ${numberOfMethods}, execution time: ${executionTime} ms`
      );
    }
    return contractsToReturn;
  }

  async addAbiToCache(address, providedAbi) {
    const cacheAbi = (newAbi) => {
      const abiHash = md5(newAbi);
      this.abiByHash[abiHash] = newAbi;
      this.abiHashByAddress[address] = abiHash;
    };
    const cachedAbi = this.getAbiFromCache(address);
    if (providedAbi) {
      cacheAbi(providedAbi);
    } else if (!cachedAbi) {
      const abi = await this.fetchAbi(address);
      cacheAbi(abi);
      await delay(this.etherscanDelayTime);
    }
  }

  getAbiFromCache(address) {
    const abiHash = this.abiHashByAddress[address];
    const abi = this.abiByHash[abiHash];
    return abi;
  }

  getReadableAbiFields(address) {
    const abi = this.getAbiFromCache(address);
    const getReadableFields = (acc, field) => {
      const { name, inputs, stateMutability, outputs } = field;
      const nbrInputs = _.size(inputs);
      const nbrOutputs = _.size(outputs);
      const hasInputs = nbrInputs > 0;
      const hasOutputs = nbrOutputs > 0;
      const viewable = stateMutability === "view";
      if (!hasInputs && hasOutputs && name && viewable) {
        acc.push(name);
      }
      return acc;
    };
    const readableFields = [];
    _.reduce(abi, getReadableFields, readableFields);
    return readableFields;
  }

  async fetchAbi(address) {
    const { etherscanApiKey } = this;
    if (etherscanApiKey === null) {
      throw new Error(
        "No etherscan API key set ser! You either need to provide an etherscan API key, or provide your own ABI in your contract config."
      );
    }

    let abi;
    let responseData;
    try {
      const url = `https://api.etherscan.io/api?module=contract&action=getabi&address=${address}&apikey=${etherscanApiKey}`;
      const response = await fetch(url);
      responseData = await response.json();
      abi = JSON.parse(responseData.result);
    } catch (err) {
      throw new Error("Etherscan error", responseData, err);
    }
    return abi;
  }
}

module.exports = BatchCall;
