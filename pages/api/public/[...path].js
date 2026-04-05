export default async function handler(req, res) {
  const handler = require('../../../api/public/[...path].js');
  return handler(req, res);
}
