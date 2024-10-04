<template>
  <v-container fluid class="pa2">
    <v-col cols="12" sm="12">
      <v-toolbar color="#0052cc" dark class="pa10">
        <v-toolbar-title class="text-center" style="flex-grow: 2">
          <h2>Recommendations for the 2026 World Cup in the USA</h2>
        </v-toolbar-title>
      </v-toolbar>

      <v-spacer></v-spacer>

      <v-container fluid class="pa2">
        <v-row justify="center">
          <v-col cols="9" sm="9">
            <v-card>
              <v-card-text>
                <span class="text-h4"
                  >Find the best options based on your location</span
                >
                <div class="text-h6">Select Stadium:</div>

                <v-carousel
                  hide-delimiters
                  v-model="selectedStadiumIndex"
                  @input="doGet"
                >
                  <v-carousel-item
                    v-for="(image, i) in images"
                    :key="i"
                    :src="image.src"
                    cover
                    @click.prevent="onImageClick(image)"
                  >
                    <v-container>
                      <v-row justify="center">
                        <v-col cols="12">
                          <span
                            class="text-h2"
                            :style="{ color: image.color }"
                            >{{ image.title }}</span
                          >
                        </v-col>
                      </v-row>
                    </v-container>
                  </v-carousel-item>
                </v-carousel>
                <br /><br />
                <v-row>
                  <v-col cols="12" md="4" sm="12">
                    <v-text-field
                      @mousedown:control="doGet"
                      v-model="category"
                      label="Where do you go?"
                      outlined
                      required
                    ></v-text-field>
                  </v-col>

                  <v-col cols="12" md="4" sm="12">
                    <div class="text-center">
                      <div class="text-caption">Budget:</div>
                      <v-rating
                        @update:model-value="doGet"
                        v-model="rating"
                        empty-icon="mdi-wallet-bifold-outline"
                        full-icon="mdi-wallet-bifold"
                        hover
                        color="green"
                      ></v-rating>
                    </div>
                  </v-col>

                  <v-col cols="12" md="4" sm="12">
                    <div>
                      <div class="text-caption">Distance in miles</div>
                      <v-slider
                        @end="doGet"
                        v-model="distancia"
                        thumb-label
                        :min="1"
                        :max="5"
                      ></v-slider>
                    </div>
                  </v-col>
                </v-row>
              </v-card-text>

              <v-row justify="center" class="pa-4">
                <v-btn @click="doGet" color="primary">Consult</v-btn>
              </v-row>
            </v-card>
          </v-col>
        </v-row>
      </v-container>
    </v-col>
  </v-container>
  <v-container fluid class="pa2" v-if="businesses != null">
    <v-row justify="center" v-if="businesses.length > 0">
      <v-col
        v-for="(business, index) in businesses"
        :key="index"
        cols="12"
        sm="6"
        md="4"
      >
        <a :href="business.url" target="_blank" style="text-decoration: none">
          <v-card class="mx-auto" max-width="400">
            <v-card-title>{{ business.name }}</v-card-title>
            <v-card-subtitle>
              Distance: {{ business.distancia_millas.toFixed(2) }} miles
            </v-card-subtitle>
            <v-card-text>
              <v-rating
                v-model="business.avg_rating"
                color="yellow"
                background-color="grey darken-1"
                half-increments
                readonly
              ></v-rating>
              <div class="text-caption">
                Google Rating: {{ business.avg_rating }}
              </div>
              <div
                class="text-caption"
                v-if="business.calificacion_ajustada_predicha"
              >
                Adjusted Rating:
                {{ business.calificacion_ajustada_predicha }}
              </div>
            </v-card-text>
          </v-card>
        </a>
      </v-col>
    </v-row>
  </v-container>
</template>

<script>
import axios from "axios";

export default {
  data() {
    return {
      selectedStadiumIndex: 0, // Selected stadium index
      distancia: 5,
      rating: 1,
      category: "Restaurant",
      businesses: null,
      images: [
        {
          src: "https://aerialsoutheast.com/wp-content/uploads/2019/01/ASCsbowl011619_126.jpg",
          title: "Mercedes-Benz Stadium",
          color: "#e7e3e2",
          id: 0,
        },
        {
          src: "https://www.gillettestadium.com/wp-content/uploads/2023/10/2023-GSLH-DIGITALKeepsake.jpeg",
          title: "Gillette Stadium",
          color: "#C70039",
          id: 1,
        },
        {
          src: "https://stadiumdb.com/pictures/stadiums/usa/cowboys_stadium/cowboys_stadium58.jpg",
          title: "AT&T Stadium",
          color: "#e7e3e2",
          id: 2,
        },
        {
          src: "https://www.noticiany.com/wp-content/uploads/2024/01/Metlife_stadium_Aerial_view-scaled.jpg?w=1200",
          title: "MetLife Stadium",
          color: "#141312",
          id: 3,
        },
        {
          src: "https://chiefsdigest.com/wp-content/uploads/2021/03/geha-field-arrowhead-768x419.png",
          title: "Arrowhead Stadium",
          color: "#e7e3e2",
          id: 4,
        },
        {
          src: "https://drupal-prod.visitcalifornia.com/sites/default/files/2021-11/VC_SoFi-Stadium_gty-1228277891-RM-1280x640.jpg", // Replace with the actual URL of SoFi Stadium
          title: "SoFi Stadium",
          color: "#141312",
          id: 6,
        },
        {
          src: "https://www.periodicocubano.com/wp-content/uploads/2024/02/El-HardRock-Stadium-de-Miami-obtiene-la-sede-de-importante-partido-del-Mundial-de-la-FIFA-2026.jpeg", // Replace with the actual URL of Hard Rock Stadium
          title: "Hard Rock Stadium",
          color: "#C70039",
          id: 7,
        },
        {
          src: "https://stadium.org/wp-content/uploads/2022/12/LumenFieldAerialB500.webp", // Replace with the actual URL of Lumen Field
          title: "Lumen Field",
          color: "#e7e3e2",
          id: 8,
        },
        {
          src: "https://assets1.afa.com.ar/torneo/01hknt32cgkccme67zjw.jpg", // Replace with the actual URL of NRG Stadium
          title: "NRG Stadium",
          color: "#e7e3e2",
          id: 9,
        },
        {
          src: "https://cdn.abcotvs.com/dip/images/13041930_032923-kgo-levis-img.jpg?w=1600", // Replace with the actual URL of Levi's Stadium
          title: "Levi's Stadium",
          color: "#e7e3e2",
          id: 10,
        },
      ],
    };
  },
  computed: {
    selectedStadiumTitle() {
      // Returns the title of the selected stadium based on the index
      return this.images[this.selectedStadiumIndex].title;
    },
  },
  methods: {
    onImageClick(image) {
      console.log(`You clicked on the stadium image: ${image.title}`);
    },
    async doGet() {
      const params = {
        stadium_cercano: this.selectedStadiumTitle || "",
        category: this.category || "", // Assign undefined if category is empty
        distancia_millas: this.distancia || 1,
        price: "$".repeat(this.rating) || "$", // Assign undefined if distance is empty
      };

      // Remove keys with undefined values
      Object.keys(params).forEach(
        (key) => params[key] === undefined && delete params[key]
      );

      try {
        // Make the GET request with axios and the parameters
        const response = await axios.get("http://localhost:8000/data", {
          params,
        });
        console.log("Server response:", response.data);
        this.businesses = response.data.data;
      } catch (error) {
        console.error("Error in the request:", error);
      }
    },
  },
};
</script>

<style lang="scss" scoped></style>
