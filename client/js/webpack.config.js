var webpack = require('webpack');
var path = require('path');

var BUILD_DIR = path.resolve(__dirname, 'build');
var APP_DIR = path.resolve(__dirname, 'src/ophion');

module.exports = {
  entry: APP_DIR + '/ophion.js',
  devtool: 'source-map',
  output: {
    library: 'ophion',
    libraryTarget: 'umd',
    path: BUILD_DIR,
    filename: 'ophion.js'
  },
  module: {
    loaders: [
      {
        test: /\.jsx?/,
        include: APP_DIR,
        loader: 'babel-loader'
      }
    ]
  }
};
