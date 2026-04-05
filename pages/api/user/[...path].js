export default async function handler(req, res) {
  const handler = require('../../../api/user/[...path].js');
  return handler(req, res);
}
