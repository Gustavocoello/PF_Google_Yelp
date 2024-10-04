import config from '@/api/config';

export default{
    getRecomendaciones : (data) => config.webApi.post('/datat' , data)
}