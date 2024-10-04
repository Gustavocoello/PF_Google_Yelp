import axios from 'axios';
import constant from '@/api/constant';

export default {   
    webApi() {
        return axios.create({ baseURL: constant.URL_BASE_API });
    },
};