<template>
  <v-layout>
    <v-app-bar class="px-md-4" color="surface-variant" flat>
      <template #prepend>
        <v-app-bar-nav-icon
          v-if="$vuetify.display.smAndDown"
          @click="drawer = !drawer"
        />
      </template>

      <v-img
        class="me-sm-8"
        max-width="40"
        src="https://cdn.vuetifyjs.com/docs/images/logos/v.svg"
      />

      <template v-if="$vuetify.display.mdAndUp">
        <v-btn
          v-for="(item, i) in items"
          :key="i"
          :active="i === 0"
          class="me-2 text-none"
          slim
          v-bind="item"
        />
      </template>

      <v-spacer />
    </v-app-bar>

    <v-navigation-drawer
      v-if="$vuetify.display.smAndDown"
      v-model="drawer"
      location="top"
      temporary
      width="355"
    >
      <v-list class="py-0" slim>
        <v-list-item link prepend-icon="mdi-home-outline" title="Dashboard" />

        <v-list-group
          prepend-icon="mdi-account-multiple-outline"
          title="Customers"
        >
          <template #activator="{ props: activatorProps }">
            <v-list-item v-bind="activatorProps" />
          </template>

          <v-list-item
            link
            prepend-icon="mdi-account-plus-outline"
            title="Create New"
          />

          <v-list-group prepend-icon="mdi-magnify" title="Search">
            <template #activator="{ props: activatorProps }">
              <v-list-item v-bind="activatorProps" />
            </template>

            <v-list-item
              link
              prepend-icon="mdi-account-outline"
              title="By Name"
            />

            <v-list-item
              link
              prepend-icon="mdi-email-outline"
              title="By Email"
            />

            <v-list-item
              link
              prepend-icon="mdi-phone-outline"
              title="By Phone"
            />
          </v-list-group>
        </v-list-group>

        <v-list-item link prepend-icon="mdi-calendar" title="Calendar" />

        <v-list-item link prepend-icon="mdi-poll" title="Analytics" />

        <v-divider />

        <v-list-item link prepend-icon="mdi-inbox-outline" title="Inbox" />

        <v-list-item
          link
          prepend-icon="mdi-bell-outline"
          title="Notifications"
        />

        <v-divider />

        <v-list-item
          lines="two"
          link
          prepend-avatar="https://vuetifyjs.b-cdn.net/docs/images/avatars/planetary-subscriber.png"
          subtitle="Vuetify Engineer"
          title="John Leider"
        />
      </v-list>
    </v-navigation-drawer>

    <v-main>
      <v-toolbar color="surface" elevation="1" height="84">
        <template #title>
          <h2 class="text-h4 font-weight-bold">Quantum</h2>
        </template>
      </v-toolbar>

      <div class="pa-4" v-show="show">
        <v-carousel hide-delimiters>
          <v-carousel-item
            v-for="(image, i) in images"
            :key="i"
            :src="image.src"
            cover
          ></v-carousel-item>
        </v-carousel>
      </div>
      <div class="text-center pa-4" v-show="show">
        <v-btn color="primary" @click="handleSelect">Seleccionar</v-btn>
      </div>

      <div v-if="show == false" class="d-flex justify-center pa-4">
        <v-form class="text-center" style="max-width: 400px">
          <v-select
            v-model="category"
            :items="categories"
            label="Selecciona una categoría"
            outlined
            required
          ></v-select>

          <v-text-field
            v-model="miles"
            label="Número de millas"
            type="number"
            outlined
            required
            
          ></v-text-field>

          <v-btn color="primary" @click="submitForm" class="mt-4">Enviar</v-btn>
        </v-form>
      </div>
    </v-main>
  </v-layout>
</template>

<script setup>
import { ref } from 'vue';

const show = ref(true); 
const category = ref(''); 
const miles = ref(''); 


const categories = ['Restaurante', 'Hotel', 'Droguería', 'Supermercado', 'Cine'];


const handleSelect = () => {
  show.value = false;
};

const items = [
  {
    text: "Recomendaciones",
  },
  {
    text: "About us",
  },
];

const images = [
  {
    src: "https://fotos.perfil.com/2024/06/06/trim/856/481/mercedes-benz-stadium-1815109.jpg?webp",
  },
  {
    src: "https://www.gillettestadium.com/wp-content/uploads/2023/10/2023-GSLH-DIGITALKeepsake.jpeg",
  },
  {
    src: "https://stadiumdb.com/pictures/stadiums/usa/cowboys_stadium/cowboys_stadium58.jpg",
  },

  {
    src: "https://assets1.afa.com.ar/torneo/01hknt32cgkccme67zjw.jpg",
  },
  {
    src: "https://chiefsdigest.com/wp-content/uploads/2021/03/geha-field-arrowhead-768x419.png",
  },
  {
    src: "https://assets1.afa.com.ar/torneo/01hknt32cgkccme67zjw.jpg",
  },
];


</script>
<style scoped>
/* Centra el contenido del formulario */
.d-flex {
  display: flex;
  align-items: center;
  justify-content: center;
}
</style>