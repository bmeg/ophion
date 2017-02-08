var webpack = require('webpack');
var path = require('path');

var BUILD_DIR = path.resolve(__dirname, 'build');
var APP_DIR = path.resolve(__dirname, 'src/ophion');

var config = {
  entry: APP_DIR + '/ophion.js',
  output: {
    path: BUILD_DIR,
    filename: 'ophion.js'
  }
};

module.exports = config;
