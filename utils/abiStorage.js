const _ = require("lodash");
const md5 = require("md5");
const delay = require("delay");
const fetch = require("cross-fetch");

class GenericStore {
  constructor() {
    this.store = {};
  }

  setItem(key, value) {
    this.store[key] = JSON.stringify(value);
  }

  getItem(key) {
    const val = this.store[key] || null;
    return JSON.parse(val);
  }
}

class Storage {
  constructor({ store, etherscanApiKey, logging }) {
    this.etherscanApiKey = etherscanApiKey;
    this.logging = logging;
    if (store) {
      this.store = store;
    } else {
      console.log("No store found, using in-memory store");
      this.store = new GenericStore();
    }
  }

  async fetchAbi(address) {
    const { etherscanApiKey } = this;
    if (!etherscanApiKey) {
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

  getAbiFromCache(address) {
    const abiHashByAddress = this.getAbiHashByAddress();
    const abiByHash = this.getAbiByHash();
    const abiHash = abiHashByAddress && abiHashByAddress[address];
    const abi = abiByHash && abiByHash[abiHash];
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
      const viewable = stateMutability === "view" || stateMutability === "pure";
      if (!hasInputs && hasOutputs && name && viewable) {
        acc.push(name);
      }
      return acc;
    };
    const readableFields = [];
    _.reduce(abi, getReadableFields, readableFields);
    return readableFields;
  }

  async addAbiToCache(address, providedAbi) {
    let abi;
    const cacheAbi = (newAbi) => {
      const abiHashByAddress = this.getAbiHashByAddress() || {};
      const abiByHash = this.getAbiByHash() || {};
      const abiHash = md5(JSON.stringify(newAbi));
      abiByHash[abiHash] = newAbi;
      abiHashByAddress[address] = abiHash;
      this.store.setItem("abiByHash", JSON.stringify(abiByHash));
      this.store.setItem("abiHashByAddress", JSON.stringify(abiHashByAddress));
      return newAbi;
    };
    const cachedAbi = this.getAbiFromCache(address);
    if (cachedAbi) {
      return cachedAbi;
    } else if (providedAbi) {
      abi = cacheAbi(providedAbi);
    } else if (!cachedAbi) {
      if (this.logging) {
        console.log(`Fetch ABI: ${address}`);
      }
      abi = await this.fetchAbi(address);
      cacheAbi(abi);
      await delay(300);
    }
    return abi;
  }

  getAbiByHash() {
    return JSON.parse(this.store.getItem("abiByHash"));
  }

  getAbiHashByAddress() {
    return JSON.parse(this.store.getItem("abiHashByAddress"));
  }
}

module.exports = Storage;
