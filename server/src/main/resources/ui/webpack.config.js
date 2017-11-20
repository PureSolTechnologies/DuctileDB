const {resolve} = require('path');
const webpack = require('webpack');
const validate = require('webpack-validator');
const {getIfUtils, removeEmpty} = require('webpack-config-utils');

var BUILD_DIR = resolve(__dirname, './build')

module.exports = env => {
  const {ifProd, ifNotProd} = getIfUtils(env)

  return validate({
    entry: ['./js/main.jsx', ],
    context: __dirname,
    output: {
      path: BUILD_DIR,
      filename: 'bundle.js',
      publicPath: '/build/',
      pathinfo: ifNotProd(),
    },
    resolve: {
        extensions: [".jsx", ".js"]
    },
    devtool: ifNotProd('source-map', 'eval'),
    devServer: {
      host: "0.0.0.0",
      port: 9090,
      historyApiFallback: true
    },
    module: {
      loaders: [
        {test: /\.jsx?$/, exclude: /node_modules/, loader: 'babel-loader'},
        {test: /\.css$/, loader: 'style-loader!css-loader'},
        {test: /(\.eot|\.woff2|\.woff|\.ttf|\.svg)$/, loader: 'file-loader'},
        {test: /\.jsx?$/, exclude: /node_modules/, loader: "source-map-loader" },
        {test: /\.html$/, exclude: /node_modules/, loader: "html-loader" }
      ],
    },
    plugins: removeEmpty([
      ifProd(new webpack.LoaderOptionsPlugin({
        minimize: false,
        debug: false,
        quiet: false,
      })),
      ifProd(new webpack.DefinePlugin({
        'process.env': {
          NODE_ENV: '"production"',
        },
      })),
      ifProd(new webpack.optimize.UglifyJsPlugin({
        sourceMap: true,
        compress: {
          screw_ie8: true, // eslint-disable-line
          warnings: false,
        },
      })),
    ]),
  });
};
