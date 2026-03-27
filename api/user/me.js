module.exports = async (req, res) => {
  const handler = require('../[...path].js');
  req.query = req.query || {};
  req.query.path = ['user', 'me'];
  return handler(req, res);
};
